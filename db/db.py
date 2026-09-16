import secrets
from enum import IntEnum
from typing import Optional, Sequence, Tuple

from sqlalchemy import Integer, String, create_engine, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class PitchieType(IntEnum):
    Brilliant = 0
    Pitched = 1
    Other = 2


class Base(DeclarativeBase):
    pass


class Death(Base):
    __tablename__ = "deaths"

    rowid: Mapped[int] = mapped_column(Integer, primary_key=True)
    server: Mapped[str] = mapped_column(String)
    channel_id: Mapped[str] = mapped_column(String)
    message_id: Mapped[str] = mapped_column(String)
    dead_person: Mapped[str] = mapped_column(String)
    caption: Mapped[str] = mapped_column(String)
    attachment: Mapped[str] = mapped_column(String)
    image_url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    timestamp: Mapped[int] = mapped_column(Integer)
    reporter: Mapped[str] = mapped_column(String)


class Pitchie(Base):
    __tablename__ = "pitchies"

    rowid: Mapped[int] = mapped_column(Integer, primary_key=True)
    server: Mapped[str] = mapped_column(String)
    channel_id: Mapped[str] = mapped_column(String)
    message_id: Mapped[str] = mapped_column(String)
    caption: Mapped[str] = mapped_column(String)
    attachment: Mapped[str] = mapped_column(String)
    image_url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    timestamp: Mapped[int] = mapped_column(Integer)
    reporter: Mapped[str] = mapped_column(String)
    type: Mapped[int] = mapped_column(Integer)


def connect_to_database(path: str) -> Session:
    engine = create_engine(f"sqlite:///{path}")
    return Session(engine)


def add_death_db(
    session: Session,
    server: str,
    channel_id: str,
    message_id: str,
    dead_person: str,
    caption: str,
    attachment: str,
    image_url: str,
    timestamp: int,
    reporter: str,
) -> int:
    death = Death(
        server=server,
        channel_id=channel_id,
        message_id=message_id,
        dead_person=dead_person,
        caption=caption,
        attachment=attachment,
        image_url=image_url,
        timestamp=timestamp,
        reporter=reporter,
    )
    session.add(death)
    session.flush()

    return death.rowid


def add_pitchie_db(
    session: Session,
    server: str,
    channel_id: str,
    message_id: str,
    caption: str,
    attachment: str,
    image_url: str,
    timestamp: int,
    reporter: str,
    type: PitchieType,
) -> int:
    pitchie = Pitchie(
        server=server,
        channel_id=channel_id,
        message_id=message_id,
        caption=caption,
        attachment=attachment,
        image_url=image_url,
        timestamp=timestamp,
        reporter=reporter,
        type=int(type),
    )
    session.add(pitchie)
    session.flush()

    return pitchie.rowid


def get_death_tally_db(session: Session, guild_id: str) -> Sequence[Tuple[str, int]]:
    stmt = (
        select(Death.dead_person, func.count(Death.rowid))
        .where(Death.server == guild_id)
        .group_by(Death.dead_person)
    )
    return session.execute(stmt).all()


def get_death_tally_time_db(session: Session, guild_id: str, start_time: int, end_time: int) -> Sequence[Tuple[str, int]]:
    stmt = (
        select(Death.dead_person, func.count(Death.rowid))
        .where(Death.server == guild_id, Death.timestamp.between(start_time, end_time))
        .group_by(Death.dead_person)
    )
    return session.execute(stmt).all()


def get_pitchie_tally_db(session: Session, guild_id: str) -> Sequence[Tuple[str, int, int]]:
    stmt = (
        select(Pitchie.reporter, Pitchie.type, func.count(Pitchie.rowid))
        .where(Pitchie.server == guild_id)
        .group_by(Pitchie.reporter, Pitchie.type)
    )
    return session.execute(stmt).all()


def get_pitchie_tally_time_db(session: Session, guild_id: str, start_time: int, end_time: int) -> Sequence[Tuple[str, int]]:
    stmt = (
        select(Pitchie.reporter, Pitchie.type, func.count(Pitchie.rowid))
        .where(Pitchie.server == guild_id, Pitchie.timestamp.between(start_time, end_time))
        .group_by(Pitchie.reporter, Pitchie.type)
    )
    return session.execute(stmt).all()


def get_death_db(session: Session, guild_id: str, dead_person: str) -> Death:
    stmt = select(Death).where(Death.server == guild_id, Death.dead_person == dead_person)
    results = session.execute(stmt).scalars().all()
    return secrets.choice(results)


def get_death_by_message_id_db(session: Session, message_id: str) -> Optional[Death]:
    stmt = select(Death).where(Death.message_id == message_id)
    # a message ID should only correspond to one death (fingers crossed)
    return session.execute(stmt).scalars().first()


def get_pitchie_by_message_id_db(session: Session, message_id: str) -> Optional[Pitchie]:
    stmt = select(Pitchie).where(Pitchie.message_id == message_id)
    # a message ID should only correspond to one pitchie (fingers crossed)
    return session.execute(stmt).scalars().first()


def update_death_image_url_db(session: Session, rowid: int, image_url: str):
    death = session.get(Death, rowid)
    death.image_url = image_url


def update_death_message_id_db(session: Session, rowid: int, message_id: str):
    death = session.get(Death, rowid)
    death.message_id = message_id


def delete_death_db(session: Session, rowid: int):
    death = session.get(Death, rowid)
    if death is not None:
        session.delete(death)


def delete_pitchie_db(session: Session, rowid: int):
    pitchie = session.get(Pitchie, rowid)
    if pitchie is not None:
        session.delete(pitchie)


def update_pitchie_image_url_db(session: Session, rowid: int, image_url: str):
    pitchie = session.get(Pitchie, rowid)
    pitchie.image_url = image_url


def update_pitchie_message_id_db(session: Session, rowid: int, message_id: str):
    pitchie = session.get(Pitchie, rowid)
    pitchie.message_id = message_id
