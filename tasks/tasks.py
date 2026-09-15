import base64
import json
import os
import time
from numbers import Number
from urllib.parse import urlparse
from typing import Any, Dict, List, Tuple

import boto3
import requests
from celery import Celery, Task
from celery.utils.log import get_task_logger

from db.db import connect_to_database, add_death_db, update_death_image_url_db, update_death_message_id_db, delete_death_db, add_pitchie_db, update_pitchie_image_url_db, update_pitchie_message_id_db, delete_pitchie_db

logger = get_task_logger(__name__)


def _log_task_event(task_name: str, event: str) -> None:
    logger.info(json.dumps({
        "timestamp": time.time(),
        "task": task_name,
        "event": event,
    }))

CELERY_BROKER = os.getenv("CELERY_BROKER")
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")
# just make sure it's defined, we don't need to pass it in below manually
if not CELERY_RESULT_BACKEND:
    raise ValueError("Missing CELERY_RESULT_BACKEND value.")

# expected to be a JSON object, e.g. {"region": "ca-central-1"}
CELERY_BROKER_TRANSPORT_OPTIONS = os.getenv("CELERY_BROKER_TRANSPORT_OPTIONS")
broker_transport_options = {}
if CELERY_BROKER_TRANSPORT_OPTIONS:
    broker_transport_options = json.loads(CELERY_BROKER_TRANSPORT_OPTIONS)
    if not isinstance(broker_transport_options, dict):
        raise ValueError("CELERY_BROKER_TRANSPORT_OPTIONS must be a JSON object.")

app = Celery("tasks", broker=CELERY_BROKER, broker_transport_options=broker_transport_options)
app.conf.worker_cancel_long_running_tasks_on_connection_loss = True # disable warning message in 5.1 <= Celery ver. < 6.0

DATABASE_PATH = os.getenv("DATABASE_PATH")
S3_BUCKET = os.getenv("S3_BUCKET")

DISCORD_BOT_APPLICATION_ID = os.getenv("DISCORD_BOT_APPLICATION_ID")
AUTHORIZATION = os.getenv("AUTHORIZATION")


@app.task
def add_death_to_db(
    server: str,
    channel_id: str,
    message_id: str,
    dead_person: str,
    caption: str,
    attachment: str,
    image_url: str,
    timestamp: Number,
    reporter: str,
) -> int:
    _log_task_event("add_death_to_db", "start")

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    rowid = add_death_db(
        cursor,
        server,
        channel_id,
        message_id,
        dead_person,
        caption,
        attachment,
        image_url,
        timestamp,
        reporter,
    )
    conn.commit()
    conn.close()

    _log_task_event("add_death_to_db", "end")

    return { "rowid": rowid }
    

@app.task
def add_pitchie_to_db(
    server: str,
    channel_id: str,
    message_id: str,
    caption: str,
    attachment: str,
    image_url: str,
    timestamp: Number,
    reporter: str,
) -> int:
    _log_task_event("add_pitchie_to_db", "start")

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    rowid = add_pitchie_db(
        cursor,
        server,
        channel_id,
        message_id,
        caption,
        attachment,
        image_url,
        timestamp,
        reporter,
    )
    conn.commit()
    conn.close()

    _log_task_event("add_pitchie_to_db", "end")

    return { "rowid": rowid }


@app.task
def download_image_and_upload_to_s3(source_url: str) -> Dict:
    _log_task_event("download_image_and_upload_to_s3", "start")

    s3 = boto3.resource("s3")

    image_name = os.path.basename(urlparse(source_url).path)
    timestamp = time.time()
    file_name = f"{timestamp}-{image_name}"
    key = f"img/{file_name}"

    response = requests.get(source_url)
    content_type = response.headers["content-type"]

    s3.Bucket(S3_BUCKET).put_object(
        Key=key,
        Body=response.content,
        ContentType=content_type,
    )
    
    s3_url = f"https://{S3_BUCKET}.s3.ca-central-1.amazonaws.com/{key}"

    _log_task_event("download_image_and_upload_to_s3", "end")

    return { "image": (file_name, content_type, s3_url) }

# combines results from a Celery group into a Dict to passed to future Tasks as a single Dict
@app.task
def gather_results(results: List[Dict], **kwargs) -> Dict:
    _log_task_event("gather_results", "start")

    for task_result in results:
        for key in task_result.keys():
            if key in kwargs:
                raise ValueError(f"duplicate key encountered: {key}")

            kwargs[key] = task_result[key]

    _log_task_event("gather_results", "end")

    return kwargs


@app.task
def update_database_with_image(input: Dict):
    _log_task_event("update_database_with_image", "start")

    rowid: int = input.get("rowid", None)
    image = input.get("image", None)

    if not rowid or not image:
        raise ValueError("missing argument")

    _, _, s3_url = image
    if not s3_url:
        raise ValueError("missing image field")

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    update_death_image_url_db(cursor, rowid, s3_url)
    conn.commit()
    conn.close()

    _log_task_event("update_database_with_image", "end")


@app.task
def update_database_with_pitchie_image(input: Dict):
    _log_task_event("update_database_with_pitchie_image", "start")

    rowid: int = input.get("rowid", None)
    image = input.get("image", None)

    if not rowid or not image:
        raise ValueError("missing argument")

    _, _, s3_url = image
    if not s3_url:
        raise ValueError("missing image field")

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    update_pitchie_image_url_db(cursor, rowid, s3_url)
    conn.commit()
    conn.close()

    _log_task_event("update_database_with_pitchie_image", "end")


# update_interaction_with_image is chained from download_image_and_upload_to_s3,
# so file_name, image_content and s3_url has to be first
@app.task(autoretry_for=(requests.exceptions.HTTPError,), default_retry_delay=5)
def update_interaction_with_image(input: Dict):
    _log_task_event("update_interaction_with_image", "start")

    image: Tuple[str, str, str, str] = input.get("image", None)
    interaction_token: str = input.get("interaction_token", None)

    if not image or not interaction_token:
        raise ValueError("missing image")
    
    file_name, file_content_type, s3_url = image
    if not file_name or not file_content_type or not s3_url:
        raise ValueError("missing image field")

    image_content = requests.get(s3_url).content

    response = requests.patch(
        f"https://discord.com/api/v10/webhooks/{DISCORD_BOT_APPLICATION_ID}/{interaction_token}/messages/@original",
        json={
            "attachments": [{"id": 0}]
        },
        headers={"Authorization": AUTHORIZATION},
        files={
            "files[0]": (file_name, image_content, file_content_type),
        },
    )

    response.raise_for_status()

    _log_task_event("update_interaction_with_image", "end")


@app.task(autoretry_for=(requests.exceptions.HTTPError,), default_retry_delay=5)
def update_database_with_message_id(input: Dict):
    _log_task_event("update_database_with_message_id", "start")

    rowid: int = input.get("rowid", None)
    interaction_token: str = input.get("interaction_token", None)

    if not rowid or not interaction_token:
        raise ValueError("missing argument")

    response = requests.get(
        f"https://discord.com/api/v10/webhooks/{DISCORD_BOT_APPLICATION_ID}/{interaction_token}/messages/@original",
        headers={"Authorization": AUTHORIZATION},
    )

    response.raise_for_status()
    message = response.json()

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    update_death_message_id_db(cursor, rowid, message["id"])
    conn.commit()
    conn.close()

    _log_task_event("update_database_with_message_id", "end")


@app.task(autoretry_for=(requests.exceptions.HTTPError,), default_retry_delay=5)
def update_database_with_pitchie_message_id(input: Dict):
    _log_task_event("update_database_with_pitchie_message_id", "start")

    rowid: int = input.get("rowid", None)
    interaction_token: str = input.get("interaction_token", None)

    if not rowid or not interaction_token:
        raise ValueError("missing argument")

    response = requests.get(
        f"https://discord.com/api/v10/webhooks/{DISCORD_BOT_APPLICATION_ID}/{interaction_token}/messages/@original",
        headers={"Authorization": AUTHORIZATION},
    )

    response.raise_for_status()
    message = response.json()

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    update_pitchie_message_id_db(cursor, rowid, message["id"])
    conn.commit()
    conn.close()

    _log_task_event("update_database_with_pitchie_message_id", "end")


@app.task
def delete_from_database(rowid: str):
    _log_task_event("delete_from_database", "start")

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    delete_death_db(cursor, rowid)
    conn.commit()
    conn.close()

    _log_task_event("delete_from_database", "end")


@app.task
def delete_pitchie_from_database(rowid: str):
    _log_task_event("delete_pitchie_from_database", "start")

    conn = connect_to_database(DATABASE_PATH)
    cursor = conn.cursor()

    delete_pitchie_db(cursor, rowid)
    conn.commit()
    conn.close()

    _log_task_event("delete_pitchie_from_database", "end")


@app.task(autoretry_for=(requests.exceptions.HTTPError,), default_retry_delay=5)
def update_message_content(channel_id: str, message_id: str, new_content: str):
    _log_task_event("update_message_content", "start")

    response = requests.patch(
        f"https://discord.com/api/v10/channels/{channel_id}/messages/{message_id}",
        json={
            "content": new_content,
        },
        headers={"Authorization": AUTHORIZATION},
    )

    response.raise_for_status()

    _log_task_event("update_message_content", "end")