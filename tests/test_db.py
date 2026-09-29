import pytest
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from db.db import (
    Base,
    Death,
    Pitchie,
    PitchieType,
    add_death_db,
    add_pitchie_db,
    connect_to_database,
    delete_death_db,
    delete_pitchie_db,
    get_death_by_message_id_db,
    get_death_db,
    get_death_tally_db,
    get_death_tally_time_db,
    get_pitchie_by_message_id_db,
    get_pitchie_tally_db,
    get_pitchie_tally_time_db,
    update_death_image_url_db,
    update_death_message_id_db,
    update_pitchie_image_url_db,
    update_pitchie_message_id_db,
)


def _add_death(session, **overrides):
    fields = dict(
        server="server1",
        channel_id="channel",
        message_id="message",
        dead_person="alice",
        caption="caption",
        attachment="attachment",
        image_url=None,
        timestamp=0,
        reporter="reporter",
    )
    fields.update(overrides)
    return add_death_db(session, **fields)


def _add_pitchie(session, **overrides):
    fields = dict(
        server="server1",
        channel_id="channel",
        message_id="message",
        caption="caption",
        attachment="attachment",
        image_url=None,
        timestamp=0,
        reporter="alice",
        type=PitchieType.Brilliant,
    )
    fields.update(overrides)
    return add_pitchie_db(session, **fields)


# connect_to_database


def test_connect_to_database_uses_sqlite_file(tmp_path):
    path = tmp_path / "test.db"
    session = connect_to_database(str(path))
    try:
        assert isinstance(session, Session)
        engine = session.get_bind()
        assert engine.dialect.name == "sqlite"
        assert engine.url.database == str(path)

        Base.metadata.create_all(engine)
        _add_death(session)
        session.commit()
        assert path.exists()
        assert set(inspect(engine).get_table_names()) == {"deaths", "pitchies"}
    finally:
        session.close()
        session.get_bind().dispose()


# add_death_db


def test_add_death_stores_all_fields(session):
    rowid = _add_death(
        session,
        server="s",
        channel_id="c",
        message_id="m",
        dead_person="p",
        caption="cap",
        attachment="att",
        image_url="url",
        timestamp=123,
        reporter="r",
    )

    death = session.get(Death, rowid)
    assert (
        death.server,
        death.channel_id,
        death.message_id,
        death.dead_person,
        death.caption,
        death.attachment,
        death.image_url,
        death.timestamp,
        death.reporter,
    ) == ("s", "c", "m", "p", "cap", "att", "url", 123, "r")


def test_add_death_returns_distinct_rowids(session):
    first = _add_death(session)
    second = _add_death(session)

    assert first is not None
    assert second is not None
    assert first != second


def test_add_death_allows_null_image_url(session):
    rowid = _add_death(session, image_url=None)

    assert session.get(Death, rowid).image_url is None


# add_pitchie_db


def test_add_pitchie_stores_all_fields(session):
    rowid = _add_pitchie(
        session,
        server="s",
        channel_id="c",
        message_id="m",
        caption="cap",
        attachment="att",
        image_url="url",
        timestamp=123,
        reporter="r",
        type=PitchieType.Other,
    )

    pitchie = session.get(Pitchie, rowid)
    assert (
        pitchie.server,
        pitchie.channel_id,
        pitchie.message_id,
        pitchie.caption,
        pitchie.attachment,
        pitchie.image_url,
        pitchie.timestamp,
        pitchie.reporter,
        pitchie.type,
    ) == ("s", "c", "m", "cap", "att", "url", 123, "r", int(PitchieType.Other))


def test_add_pitchie_stores_type_as_int(session):
    for pitchie_type in PitchieType:
        rowid = _add_pitchie(session, type=pitchie_type)
        stored = session.get(Pitchie, rowid).type
        assert type(stored) is int
        assert stored == pitchie_type.value


def test_add_pitchie_returns_distinct_rowids(session):
    assert _add_pitchie(session) != _add_pitchie(session)


# get_death_tally_db


def test_death_tally_is_per_server(session):
    _add_death(session, server="server1", dead_person="alice")
    _add_death(session, server="server1", dead_person="alice")
    _add_death(session, server="server1", dead_person="bob")
    _add_death(session, server="server2", dead_person="alice")

    assert sorted(get_death_tally_db(session, "server1")) == [("alice", 2), ("bob", 1)]
    assert sorted(get_death_tally_db(session, "server2")) == [("alice", 1)]


def test_death_tally_empty_server(session):
    _add_death(session, server="server1")

    assert get_death_tally_db(session, "nope") == []


# get_death_tally_time_db


def test_death_tally_time_filters_by_timestamp(session):
    _add_death(session, dead_person="alice", timestamp=5)
    _add_death(session, dead_person="alice", timestamp=15)
    _add_death(session, dead_person="bob", timestamp=25)

    assert sorted(get_death_tally_time_db(session, "server1", 10, 30)) == [("alice", 1), ("bob", 1)]


def test_death_tally_time_bounds_are_inclusive(session):
    _add_death(session, dead_person="alice", timestamp=10)
    _add_death(session, dead_person="alice", timestamp=20)
    _add_death(session, dead_person="alice", timestamp=21)

    assert get_death_tally_time_db(session, "server1", 10, 20) == [("alice", 2)]


def test_death_tally_time_filters_by_server(session):
    _add_death(session, server="server1", timestamp=10)
    _add_death(session, server="server2", timestamp=10)

    assert get_death_tally_time_db(session, "server2", 0, 100) == [("alice", 1)]


def test_death_tally_time_no_matches(session):
    _add_death(session, timestamp=100)

    assert get_death_tally_time_db(session, "server1", 0, 50) == []


# get_pitchie_tally_db


def test_pitchie_tally_groups_by_reporter_and_type(session):
    _add_pitchie(session, reporter="alice", type=PitchieType.Brilliant)
    _add_pitchie(session, reporter="alice", type=PitchieType.Brilliant)
    _add_pitchie(session, reporter="alice", type=PitchieType.Pitched)
    _add_pitchie(session, reporter="bob", type=PitchieType.Other)

    assert sorted(get_pitchie_tally_db(session, "server1")) == [
        ("alice", int(PitchieType.Brilliant), 2),
        ("alice", int(PitchieType.Pitched), 1),
        ("bob", int(PitchieType.Other), 1),
    ]


def test_pitchie_tally_is_per_server(session):
    _add_pitchie(session, server="server1")
    _add_pitchie(session, server="server2")
    _add_pitchie(session, server="server2")

    assert get_pitchie_tally_db(session, "server2") == [("alice", int(PitchieType.Brilliant), 2)]
    assert get_pitchie_tally_db(session, "nope") == []


# get_pitchie_tally_time_db


def test_pitchie_tally_time_filters_by_timestamp(session):
    _add_pitchie(session, timestamp=5)
    _add_pitchie(session, timestamp=10)
    _add_pitchie(session, timestamp=20)
    _add_pitchie(session, timestamp=21)

    assert get_pitchie_tally_time_db(session, "server1", 10, 20) == [("alice", int(PitchieType.Brilliant), 2)]


def test_pitchie_tally_time_filters_by_server(session):
    _add_pitchie(session, server="server1", timestamp=10)
    _add_pitchie(session, server="server2", timestamp=10)

    assert get_pitchie_tally_time_db(session, "server1", 0, 100) == [("alice", int(PitchieType.Brilliant), 1)]


def test_pitchie_tally_time_groups_by_type(session):
    _add_pitchie(session, type=PitchieType.Brilliant, timestamp=10)
    _add_pitchie(session, type=PitchieType.Pitched, timestamp=10)

    assert sorted(get_pitchie_tally_time_db(session, "server1", 0, 100)) == [
        ("alice", int(PitchieType.Brilliant), 1),
        ("alice", int(PitchieType.Pitched), 1),
    ]


# get_death_db


def test_get_death_returns_matching_death(session):
    rowid = _add_death(session, dead_person="alice")
    _add_death(session, dead_person="bob")
    _add_death(session, server="server2", dead_person="alice")

    death = get_death_db(session, "server1", "alice")

    assert death.rowid == rowid


def test_get_death_picks_among_all_matches(session, monkeypatch):
    rowids = {_add_death(session, dead_person="alice") for _ in range(3)}
    _add_death(session, dead_person="bob")

    seen = []
    monkeypatch.setattr("db.db.secrets.choice", lambda results: seen.extend(results) or results[-1])
    death = get_death_db(session, "server1", "alice")

    assert {d.rowid for d in seen} == rowids
    assert death.rowid in rowids


@pytest.mark.xfail(raises=IndexError, strict=True, reason="secrets.choice raises on an empty result list")
def test_get_death_no_matches_returns_none(session):
    _add_death(session, dead_person="bob")

    assert get_death_db(session, "server1", "alice") is None


# get_death_by_message_id_db


def test_get_death_by_message_id(session):
    rowid = _add_death(session, message_id="m1")
    _add_death(session, message_id="m2")

    assert get_death_by_message_id_db(session, "m1").rowid == rowid


def test_get_death_by_message_id_missing(session):
    _add_death(session, message_id="m1")

    assert get_death_by_message_id_db(session, "nope") is None


# get_pitchie_by_message_id_db


def test_get_pitchie_by_message_id(session):
    rowid = _add_pitchie(session, message_id="m1")
    _add_pitchie(session, message_id="m2")

    assert get_pitchie_by_message_id_db(session, "m1").rowid == rowid


def test_get_pitchie_by_message_id_missing(session):
    _add_pitchie(session, message_id="m1")

    assert get_pitchie_by_message_id_db(session, "nope") is None


def test_get_by_message_id_does_not_cross_tables(session):
    _add_death(session, message_id="death-msg")
    _add_pitchie(session, message_id="pitchie-msg")

    assert get_death_by_message_id_db(session, "pitchie-msg") is None
    assert get_pitchie_by_message_id_db(session, "death-msg") is None


# update_death_image_url_db / update_death_message_id_db


def test_update_death_image_url(session):
    rowid = _add_death(session, image_url=None)
    other = _add_death(session, image_url=None)

    update_death_image_url_db(session, rowid, "https://example.com/a.png")
    session.flush()

    assert session.get(Death, rowid).image_url == "https://example.com/a.png"
    assert session.get(Death, other).image_url is None


def test_update_death_message_id(session):
    rowid = _add_death(session, message_id="old")

    update_death_message_id_db(session, rowid, "new")
    session.flush()

    assert get_death_by_message_id_db(session, "new").rowid == rowid
    assert get_death_by_message_id_db(session, "old") is None


@pytest.mark.xfail(raises=AttributeError, strict=True, reason="session.get returns None for a missing rowid")
def test_update_death_image_url_missing_rowid_is_noop(session):
    _add_death(session)

    update_death_image_url_db(session, 9999, "https://example.com/a.png")


@pytest.mark.xfail(raises=AttributeError, strict=True, reason="session.get returns None for a missing rowid")
def test_update_death_message_id_missing_rowid_is_noop(session):
    _add_death(session)

    update_death_message_id_db(session, 9999, "new")


# update_pitchie_image_url_db / update_pitchie_message_id_db


def test_update_pitchie_image_url(session):
    rowid = _add_pitchie(session, image_url=None)
    other = _add_pitchie(session, image_url=None)

    update_pitchie_image_url_db(session, rowid, "https://example.com/a.png")
    session.flush()

    assert session.get(Pitchie, rowid).image_url == "https://example.com/a.png"
    assert session.get(Pitchie, other).image_url is None


def test_update_pitchie_message_id(session):
    rowid = _add_pitchie(session, message_id="old")

    update_pitchie_message_id_db(session, rowid, "new")
    session.flush()

    assert get_pitchie_by_message_id_db(session, "new").rowid == rowid
    assert get_pitchie_by_message_id_db(session, "old") is None


@pytest.mark.xfail(raises=AttributeError, strict=True, reason="session.get returns None for a missing rowid")
def test_update_pitchie_image_url_missing_rowid_is_noop(session):
    _add_pitchie(session)

    update_pitchie_image_url_db(session, 9999, "https://example.com/a.png")


@pytest.mark.xfail(raises=AttributeError, strict=True, reason="session.get returns None for a missing rowid")
def test_update_pitchie_message_id_missing_rowid_is_noop(session):
    _add_pitchie(session)

    update_pitchie_message_id_db(session, 9999, "new")


# delete_death_db / delete_pitchie_db


def test_delete_death(session):
    rowid = _add_death(session, message_id="m1")
    other = _add_death(session, message_id="m2")

    delete_death_db(session, rowid)
    session.flush()

    assert session.get(Death, rowid) is None
    assert get_death_by_message_id_db(session, "m1") is None
    assert session.get(Death, other) is not None


def test_delete_death_missing_rowid_is_noop(session):
    _add_death(session)

    delete_death_db(session, 9999)
    session.flush()

    assert get_death_tally_db(session, "server1") == [("alice", 1)]


def test_delete_pitchie(session):
    rowid = _add_pitchie(session, message_id="m1")
    other = _add_pitchie(session, message_id="m2")

    delete_pitchie_db(session, rowid)
    session.flush()

    assert session.get(Pitchie, rowid) is None
    assert get_pitchie_by_message_id_db(session, "m1") is None
    assert session.get(Pitchie, other) is not None


def test_delete_pitchie_missing_rowid_is_noop(session):
    _add_pitchie(session)

    delete_pitchie_db(session, 9999)
    session.flush()

    assert get_pitchie_tally_db(session, "server1") == [("alice", int(PitchieType.Brilliant), 1)]
