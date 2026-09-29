import os
import re

import pytest
import requests
from celery.canvas import chord

# tasks.tasks reads its config at import time
os.environ.setdefault("CELERY_BROKER", "memory://")
os.environ.setdefault("CELERY_RESULT_BACKEND", "cache+memory://")

import tasks.tasks as app_tasks  # noqa: E402


class FakeResponse:
    def __init__(self, json_data=None, content=b"", status_code=200):
        self._json_data = json_data
        self.content = content
        self.status_code = status_code

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"status {self.status_code}")


class Replaced(Exception):
    def __init__(self, sig):
        self.sig = sig


@pytest.fixture
def capture_replace(monkeypatch):
    def install(task):
        def fake_replace(sig):
            raise Replaced(sig)

        monkeypatch.setattr(task, "replace", fake_replace)

    return install


def _member(nick=None, avatar=None, global_name=None, username="user", user_avatar=None):
    return {
        "nick": nick,
        "avatar": avatar,
        "user": {"id": "u1", "username": username, "global_name": global_name, "avatar": user_avatar},
    }


# get_gallery_user_info


@pytest.mark.parametrize("member, expected", [
    (_member(nick="Nick", global_name="Global", username="user"), "Nick"),
    (_member(global_name="Global", username="user"), "Global"),
    (_member(username="user"), "user"),
])
def test_user_info_display_name_fallbacks(monkeypatch, member, expected):
    monkeypatch.setattr(app_tasks.requests, "get", lambda *a, **kw: FakeResponse(member))

    result = app_tasks.get_gallery_user_info.run("g1", "u1", False)

    assert result == {"user_id": "u1", "display_name": expected, "profile_picture_url": None}


def test_user_info_reporter_failure_returns_unknown_user(monkeypatch):
    monkeypatch.setattr(app_tasks.requests, "get", lambda *a, **kw: FakeResponse(status_code=404))

    result = app_tasks.get_gallery_user_info.run("g1", "u1", False)

    assert result == {"user_id": "u1", "display_name": "Unknown user", "profile_picture_url": None}


def test_user_info_target_failure_raises(monkeypatch):
    monkeypatch.setattr(app_tasks.requests, "get", lambda *a, **kw: FakeResponse(status_code=404))

    with pytest.raises(requests.exceptions.HTTPError):
        app_tasks.get_gallery_user_info.run("g1", "u1", True)


@pytest.mark.parametrize("member, expected_avatar_url", [
    (_member(avatar="guildhash", user_avatar="userhash"), "https://cdn.discordapp.com/guilds/g1/users/u1/avatars/guildhash.png?size=256"),
    (_member(user_avatar="userhash"), "https://cdn.discordapp.com/avatars/u1/userhash.png?size=256"),
])
def test_user_info_target_uploads_profile_picture(monkeypatch, member, expected_avatar_url):
    uploaded = []
    monkeypatch.setattr(app_tasks.requests, "get", lambda *a, **kw: FakeResponse(member))
    monkeypatch.setattr(app_tasks, "_upload_profile_picture_to_s3", lambda url: uploaded.append(url) or "https://s3/pic.png")

    result = app_tasks.get_gallery_user_info.run("g1", "u1", True)

    assert uploaded == [expected_avatar_url]
    assert result["profile_picture_url"] == "https://s3/pic.png"


def test_user_info_target_without_avatar_has_no_profile_picture(monkeypatch):
    monkeypatch.setattr(app_tasks.requests, "get", lambda *a, **kw: FakeResponse(_member()))
    monkeypatch.setattr(app_tasks, "_upload_profile_picture_to_s3", lambda url: pytest.fail("should not upload"))

    assert app_tasks.get_gallery_user_info.run("g1", "u1", True)["profile_picture_url"] is None


# workflow construction


def test_start_workflow_looks_up_target_before_reporters(monkeypatch, capture_replace):
    death = lambda reporter, ts: type("Death", (), dict(caption="c", image_url="https://img", timestamp=ts, reporter=reporter))
    monkeypatch.setattr(app_tasks, "connect_to_database", lambda path: type("S", (), {"close": lambda self: None})())
    monkeypatch.setattr(app_tasks, "get_deaths_for_person_db", lambda s, g, p: [death("r1", 1), death("r2", 2), death("r1", 3)])
    capture_replace(app_tasks.start_gallery_workflow)

    with pytest.raises(Replaced) as replaced:
        app_tasks.start_gallery_workflow.run("g1", "target", "token")

    first, second = replaced.value.sig.tasks
    assert first.task == app_tasks.get_gallery_user_info.name
    assert tuple(first.args) == ("g1", "target", True)
    assert second.task == app_tasks.dispatch_gallery_reporter_lookups.name
    _, _, _, deaths, reporter_ids = second.args
    assert reporter_ids == ["r1", "r2"]
    assert [d["timestamp"] for d in deaths] == [1, 2, 3]


def test_dispatch_builds_chord_over_reporters(capture_replace):
    capture_replace(app_tasks.dispatch_gallery_reporter_lookups)
    target_info = {"user_id": "target", "display_name": "T", "profile_picture_url": None}

    with pytest.raises(Replaced) as replaced:
        app_tasks.dispatch_gallery_reporter_lookups.run(target_info, "g1", "target", "token", [], ["r1", "r2"])

    # celery folds `chord | sig` into a chord whose body is the chain
    gallery_chord = replaced.value.sig
    assert isinstance(gallery_chord, chord)
    assert [tuple(t.args) for t in gallery_chord.tasks] == [("g1", "r1", False), ("g1", "r2", False)]
    generate, update = gallery_chord.body.tasks
    assert generate.task == app_tasks.generate_gallery_page.name
    assert update.task == app_tasks.update_interaction_with_gallery.name
    assert tuple(update.args) == ("target", "token")


def test_dispatch_without_reporters_skips_chord(capture_replace):
    capture_replace(app_tasks.dispatch_gallery_reporter_lookups)
    target_info = {"user_id": "target", "display_name": "T", "profile_picture_url": None}

    with pytest.raises(Replaced) as replaced:
        app_tasks.dispatch_gallery_reporter_lookups.run(target_info, "g1", "target", "token", [], [])

    generate, _ = replaced.value.sig.tasks
    assert generate.task == app_tasks.generate_gallery_page.name
    assert tuple(generate.args) == ([], target_info, [])


# gallery page


def test_render_gallery_page_maps_reporters_and_escapes():
    html = app_tasks.render_gallery_page(
        [
            {"user_id": "r1", "display_name": "Reporter One", "profile_picture_url": None},
            {"user_id": "r2", "display_name": "Unknown user", "profile_picture_url": None},
        ],
        {"user_id": "target", "display_name": "Target", "profile_picture_url": "https://s3/pic.png"},
        [
            {"caption": "<script>alert(1)</script>", "image_url": "https://img/1.png", "timestamp": 0, "reporter": "r1"},
            {"caption": "self report", "image_url": "https://img/2.png", "timestamp": 60, "reporter": "target"},
        ],
        0,
    )

    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html
    assert "Reported by Reporter One on 1970-01-01 00:00 UTC" in html
    assert "Reported by Target on 1970-01-01 00:01 UTC" in html
    assert 'src="https://s3/pic.png"' in html
    assert 'id="death-1"' in html


def test_generate_gallery_page_writes_file(monkeypatch, tmp_path):
    monkeypatch.setattr(app_tasks.config, "GALLERY_PAGES_OUTPUT_DIRECTORY", str(tmp_path))
    target_info = {"user_id": "target", "display_name": "Target", "profile_picture_url": None}

    link = app_tasks.generate_gallery_page.run([], target_info, [])

    match = re.fullmatch(r"https://rip-bot\.com/gallery/([A-Za-z0-9]{10})\.html", link)
    assert match
    assert "Target" in (tmp_path / f"{match.group(1)}.html").read_text(encoding="utf-8")
