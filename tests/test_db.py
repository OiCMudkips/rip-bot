from db.db import (
    PitchieType,
    add_death_db,
    add_pitchie_db,
    delete_death_db,
    get_death_by_message_id_db,
    get_death_tally_db,
    get_pitchie_tally_db,
)


def _add_death(session, server, dead_person, message_id):
    return add_death_db(
        session,
        server=server,
        channel_id="channel",
        message_id=message_id,
        dead_person=dead_person,
        caption="caption",
        attachment="attachment",
        image_url=None,
        timestamp=0,
        reporter="reporter",
    )


def _add_pitchie(session, server, reporter, type):
    return add_pitchie_db(
        session,
        server=server,
        channel_id="channel",
        message_id="message",
        caption="caption",
        attachment="attachment",
        image_url=None,
        timestamp=0,
        reporter=reporter,
        type=type,
    )


def test_death_tally_is_per_server(session):
    _add_death(session, "server1", "alice", "m1")
    _add_death(session, "server1", "alice", "m2")
    _add_death(session, "server1", "bob", "m3")
    _add_death(session, "server2", "alice", "m4")

    assert sorted(get_death_tally_db(session, "server1")) == [("alice", 2), ("bob", 1)]
    assert sorted(get_death_tally_db(session, "server2")) == [("alice", 1)]


def test_pitchie_tally_groups_by_type(session):
    _add_pitchie(session, "server1", "alice", PitchieType.Brilliant)
    _add_pitchie(session, "server1", "alice", PitchieType.Brilliant)
    _add_pitchie(session, "server1", "alice", PitchieType.Pitched)

    assert sorted(get_pitchie_tally_db(session, "server1")) == [
        ("alice", int(PitchieType.Brilliant), 2),
        ("alice", int(PitchieType.Pitched), 1),
    ]


def test_delete_death(session):
    rowid = _add_death(session, "server1", "alice", "m1")
    delete_death_db(session, rowid)

    assert get_death_by_message_id_db(session, "m1") is None
