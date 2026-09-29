import base64
import json
import os
import secrets
import string
import time
from datetime import datetime, timezone
from numbers import Number
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional, Tuple

import boto3
import chevron
import requests
from betterconf import betterconf, field
from celery import Celery, Task, chord, group
from celery.utils.log import get_task_logger

from db.db import connect_to_database, add_death_db, update_death_image_url_db, update_death_message_id_db, delete_death_db, add_pitchie_db, update_pitchie_image_url_db, update_pitchie_message_id_db, delete_pitchie_db, get_deaths_for_person_db

logger = get_task_logger(__name__)


def _log_task_event(task_name: str, event: str) -> None:
    logger.info(json.dumps({
        "timestamp": time.time(),
        "task": task_name,
        "event": event,
    }))


@betterconf
class TasksConfig:
    CELERY_BROKER: str = field(default=None)
    CELERY_RESULT_BACKEND: str = field(default=None)
    # expected to be a JSON object, e.g. {"region": "ca-central-1"}
    CELERY_BROKER_TRANSPORT_OPTIONS: str = field(default=None)
    DATABASE_PATH: str = field(default=None)
    S3_BUCKET: str = field(default=None)
    DISCORD_BOT_APPLICATION_ID: str = field(default=None)
    AUTHORIZATION: str = field(default=None)
    S3_BUCKET_USER_PROFILE_PICTURES: str = field(default=None)
    GALLERY_PAGES_OUTPUT_DIRECTORY: str = field(default=None)


config = TasksConfig()

# just make sure it's defined, we don't need to pass it in below manually
if not config.CELERY_RESULT_BACKEND:
    raise ValueError("Missing CELERY_RESULT_BACKEND value.")

broker_transport_options = {}
if config.CELERY_BROKER_TRANSPORT_OPTIONS:
    broker_transport_options = json.loads(config.CELERY_BROKER_TRANSPORT_OPTIONS)
    if not isinstance(broker_transport_options, dict):
        raise ValueError("CELERY_BROKER_TRANSPORT_OPTIONS must be a JSON object.")

app = Celery("tasks", broker=config.CELERY_BROKER, broker_transport_options=broker_transport_options)
app.conf.worker_cancel_long_running_tasks_on_connection_loss = True # disable warning message in 5.1 <= Celery ver. < 6.0


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

    session = connect_to_database(config.DATABASE_PATH)

    rowid = add_death_db(
        session,
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
    session.commit()
    session.close()

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
    type: int,
) -> int:
    _log_task_event("add_pitchie_to_db", "start")

    session = connect_to_database(config.DATABASE_PATH)

    rowid = add_pitchie_db(
        session,
        server,
        channel_id,
        message_id,
        caption,
        attachment,
        image_url,
        timestamp,
        reporter,
        type,
    )
    session.commit()
    session.close()

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

    s3.Bucket(config.S3_BUCKET).put_object(
        Key=key,
        Body=response.content,
        ContentType=content_type,
    )
    
    s3_url = f"https://{config.S3_BUCKET}.s3.ca-central-1.amazonaws.com/{key}"

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

    session = connect_to_database(config.DATABASE_PATH)

    update_death_image_url_db(session, rowid, s3_url)
    session.commit()
    session.close()

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

    session = connect_to_database(config.DATABASE_PATH)

    update_pitchie_image_url_db(session, rowid, s3_url)
    session.commit()
    session.close()

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
        f"https://discord.com/api/v10/webhooks/{config.DISCORD_BOT_APPLICATION_ID}/{interaction_token}/messages/@original",
        json={
            "attachments": [{"id": 0}]
        },
        headers={"Authorization": config.AUTHORIZATION},
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
        f"https://discord.com/api/v10/webhooks/{config.DISCORD_BOT_APPLICATION_ID}/{interaction_token}/messages/@original",
        headers={"Authorization": config.AUTHORIZATION},
    )

    response.raise_for_status()
    message = response.json()

    session = connect_to_database(config.DATABASE_PATH)

    update_death_message_id_db(session, rowid, message["id"])
    session.commit()
    session.close()

    _log_task_event("update_database_with_message_id", "end")


@app.task(autoretry_for=(requests.exceptions.HTTPError,), default_retry_delay=5)
def update_database_with_pitchie_message_id(input: Dict):
    _log_task_event("update_database_with_pitchie_message_id", "start")

    rowid: int = input.get("rowid", None)
    interaction_token: str = input.get("interaction_token", None)

    if not rowid or not interaction_token:
        raise ValueError("missing argument")

    response = requests.get(
        f"https://discord.com/api/v10/webhooks/{config.DISCORD_BOT_APPLICATION_ID}/{interaction_token}/messages/@original",
        headers={"Authorization": config.AUTHORIZATION},
    )

    response.raise_for_status()
    message = response.json()

    session = connect_to_database(config.DATABASE_PATH)

    update_pitchie_message_id_db(session, rowid, message["id"])
    session.commit()
    session.close()

    _log_task_event("update_database_with_pitchie_message_id", "end")


@app.task
def delete_from_database(rowid: str):
    _log_task_event("delete_from_database", "start")

    session = connect_to_database(config.DATABASE_PATH)

    delete_death_db(session, rowid)
    session.commit()
    session.close()

    _log_task_event("delete_from_database", "end")


@app.task
def delete_pitchie_from_database(rowid: str):
    _log_task_event("delete_pitchie_from_database", "start")

    session = connect_to_database(config.DATABASE_PATH)

    delete_pitchie_db(session, rowid)
    session.commit()
    session.close()

    _log_task_event("delete_pitchie_from_database", "end")


@app.task(autoretry_for=(requests.exceptions.HTTPError,), default_retry_delay=5)
def update_message_content(channel_id: str, message_id: str, new_content: str):
    _log_task_event("update_message_content", "start")

    response = requests.patch(
        f"https://discord.com/api/v10/channels/{channel_id}/messages/{message_id}",
        json={
            "content": new_content,
        },
        headers={"Authorization": config.AUTHORIZATION},
    )

    response.raise_for_status()

    _log_task_event("update_message_content", "end")


# ---- gallery ----

GALLERY_URL_TEMPLATE = "https://rip-bot.com/gallery/{page_id}.html"
GALLERY_GENERATED_MESSAGE_TEMPLATE = "Your gallery for <@{target_user_id}> has been generated! [Link]({link})"
GALLERY_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "templates", "gallery.mustache")
GALLERY_PAGE_ID_ALPHABET = string.ascii_letters + string.digits
GALLERY_PAGE_ID_LENGTH = 10
UNKNOWN_USER_DISPLAY_NAME = "Unknown user"
# the whole reporter group has to finish well within the 15 minute interaction token lifetime.
# the soft limit raises inside the task so it can fall back to "Unknown user", the hard limit is a backstop
USER_INFO_SOFT_TIME_LIMIT = 5 * 60
USER_INFO_TIME_LIMIT = USER_INFO_SOFT_TIME_LIMIT + 10
REQUEST_TIMEOUT = 30


def _format_timestamp(timestamp: Number) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


@app.task(bind=True)
def start_gallery_workflow(self: Task, guild_id: str, target_user_id: str, interaction_token: str):
    _log_task_event("start_gallery_workflow", "start")

    session = connect_to_database(config.DATABASE_PATH)
    deaths = [
        {
            "caption": death.caption,
            "image_url": death.image_url,
            "timestamp": death.timestamp,
            "reporter": death.reporter,
        }
        for death in get_deaths_for_person_db(session, guild_id, target_user_id)
        if death.image_url
    ]
    session.close()

    reporter_ids = list(dict.fromkeys(death["reporter"] for death in deaths))

    _log_task_event("start_gallery_workflow", "end")

    # the target user's lookup runs to completion first; if it fails the chain aborts
    # before any reporter lookups are created
    raise self.replace(
        get_gallery_user_info.si(guild_id, target_user_id, True) |
        dispatch_gallery_reporter_lookups.s(guild_id, target_user_id, interaction_token, deaths, reporter_ids)
    )


@app.task(bind=True)
def dispatch_gallery_reporter_lookups(
    self: Task,
    target_user_info: Dict,
    guild_id: str,
    target_user_id: str,
    interaction_token: str,
    deaths: List[Dict],
    reporter_ids: List[str],
):
    _log_task_event("dispatch_gallery_reporter_lookups", "start")

    if reporter_ids:
        gallery = chord(
            group(get_gallery_user_info.si(guild_id, reporter_id, False) for reporter_id in reporter_ids),
            generate_gallery_page.s(target_user_info, deaths),
        )
    else:
        # a chord with an empty header is not reliably supported, so skip straight to the callback
        gallery = generate_gallery_page.si([], target_user_info, deaths)

    _log_task_event("dispatch_gallery_reporter_lookups", "end")

    raise self.replace(gallery | update_interaction_with_gallery.s(target_user_id, interaction_token))


def _fetch_guild_member(guild_id: str, user_id: str) -> Dict:
    response = requests.get(
        f"https://discord.com/api/v10/guilds/{guild_id}/members/{user_id}",
        headers={"Authorization": config.AUTHORIZATION},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def _get_avatar_url(guild_id: str, user_id: str, member: Dict) -> Optional[str]:
    # https://docs.discord.com/developers/reference#image-formatting
    if member.get("avatar"):
        return f"https://cdn.discordapp.com/guilds/{guild_id}/users/{user_id}/avatars/{member['avatar']}.png?size=256"

    user_avatar = member.get("user", {}).get("avatar")
    if user_avatar:
        return f"https://cdn.discordapp.com/avatars/{user_id}/{user_avatar}.png?size=256"

    return None


def _upload_profile_picture_to_s3(avatar_url: str) -> str:
    response = requests.get(avatar_url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    key = f"{secrets.token_hex(16)}.png"
    s3 = boto3.resource("s3")
    s3.Bucket(config.S3_BUCKET_USER_PROFILE_PICTURES).put_object(
        Key=key,
        Body=response.content,
        ContentType="image/png",
        ACL="public-read",
    )

    return f"https://{config.S3_BUCKET_USER_PROFILE_PICTURES}.s3.ca-central-1.amazonaws.com/{key}"


@app.task(soft_time_limit=USER_INFO_SOFT_TIME_LIMIT, time_limit=USER_INFO_TIME_LIMIT)
def get_gallery_user_info(guild_id: str, user_id: str, is_target_user: bool) -> Dict:
    _log_task_event("get_gallery_user_info", "start")

    try:
        member = _fetch_guild_member(guild_id, user_id)
        user = member.get("user", {})
        display_name = member.get("nick") or user.get("global_name") or user.get("username") or UNKNOWN_USER_DISPLAY_NAME

        profile_picture_url = None
        if is_target_user:
            avatar_url = _get_avatar_url(guild_id, user_id, member)
            if avatar_url:
                profile_picture_url = _upload_profile_picture_to_s3(avatar_url)
    except Exception:  # includes SoftTimeLimitExceeded
        if is_target_user:
            # cancels the rest of the gallery workflow
            raise

        logger.exception(f"failed to get user info for {user_id}")
        display_name = UNKNOWN_USER_DISPLAY_NAME
        profile_picture_url = None

    _log_task_event("get_gallery_user_info", "end")

    return {
        "user_id": user_id,
        "display_name": display_name,
        "profile_picture_url": profile_picture_url,
    }


def render_gallery_page(reporter_infos: List[Dict], target_user_info: Dict, deaths: List[Dict], generated_at: Number) -> str:
    display_names = {info["user_id"]: info["display_name"] for info in reporter_infos}
    display_names[target_user_info["user_id"]] = target_user_info["display_name"]

    with open(GALLERY_TEMPLATE_PATH, encoding="utf-8") as template:
        return chevron.render(template, {
            "display_name": target_user_info["display_name"],
            "profile_picture_url": target_user_info["profile_picture_url"],
            "generated_at": _format_timestamp(generated_at),
            "death_count": len(deaths),
            "deaths": [
                {
                    "id": index,
                    "image_url": death["image_url"],
                    "caption": death["caption"],
                    "reported_at": _format_timestamp(death["timestamp"]),
                    "reporter_name": display_names.get(death["reporter"], UNKNOWN_USER_DISPLAY_NAME),
                }
                for index, death in enumerate(deaths)
            ],
        })


@app.task
def generate_gallery_page(reporter_infos: List[Dict], target_user_info: Dict, deaths: List[Dict]) -> str:
    _log_task_event("generate_gallery_page", "start")

    html = render_gallery_page(reporter_infos, target_user_info, deaths, time.time())

    page_id = "".join(secrets.choice(GALLERY_PAGE_ID_ALPHABET) for _ in range(GALLERY_PAGE_ID_LENGTH))
    output_path = os.path.join(config.GALLERY_PAGES_OUTPUT_DIRECTORY, f"{page_id}.html")
    # "x" so we never overwrite an existing gallery
    with open(output_path, "x", encoding="utf-8") as output_file:
        output_file.write(html)

    _log_task_event("generate_gallery_page", "end")

    return GALLERY_URL_TEMPLATE.format(page_id=page_id)


@app.task(autoretry_for=(requests.exceptions.HTTPError,), default_retry_delay=5)
def update_interaction_with_gallery(link: str, target_user_id: str, interaction_token: str):
    _log_task_event("update_interaction_with_gallery", "start")

    response = requests.patch(
        f"https://discord.com/api/v10/webhooks/{config.DISCORD_BOT_APPLICATION_ID}/{interaction_token}/messages/@original",
        json={
            "content": GALLERY_GENERATED_MESSAGE_TEMPLATE.format(target_user_id=target_user_id, link=link),
        },
        headers={"Authorization": config.AUTHORIZATION},
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()

    _log_task_event("update_interaction_with_gallery", "end")