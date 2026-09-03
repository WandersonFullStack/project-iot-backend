from __future__ import annotations
from datetime import datetime, timezone

from sqlalchemy import select, update, delete, func, distinct
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema_orm import Device, ReceivedMessage, Publication

async def insert_message(
        db: AsyncSession,
        device_id: str | None,
        topic: str,
        payload: str,
        qos: int,
        retain: bool,
        content_type: str | None = None,
        user_props: str | None = None,
) -> ReceivedMessage:
    message = ReceivedMessage(
        device_id=device_id,
        topic=topic,
        payload=payload,
        qos=qos,
        retain=retain,
        content_type=content_type,
        user_props=user_props,
    )
    db.add(message)
    await db.flush()

    return message

async def list_message(
        db: AsyncSession,
        user_id: int,
        topic: str | None = None,
        limit: int = 10,
        offset: int = 0,
) -> list[ReceivedMessage]:
    stmt = (
        select(ReceivedMessage)
        .join(Device, ReceivedMessage.device_id == Device.device_id)
        .where(Device.user_id == user_id)
        .order_by(ReceivedMessage.id.desc())
        .limit(limit)
        .offset(offset)
    )

    if topic:
        stmt = stmt.where(ReceivedMessage.topic.like(topic))

    result = await db.execute(stmt)
    return list(result.scalars().all())

async def search_message(
        db: AsyncSession,
        message_id: int,
        user_id: int,
) -> ReceivedMessage | None:
    result = await db.execute(
        select(ReceivedMessage)
        .join(Device, ReceivedMessage.device_id == Device.device_id)
        .where(
            ReceivedMessage.id == message_id,
            Device.user_id == user_id,
        )
    )

    return result.scalar_one_or_none()

async def delete_message(
        db: AsyncSession,
        message_id: int,
        user_id: int,
) -> bool:
    result = await db.execute(
        delete(ReceivedMessage)
        .where(
            ReceivedMessage.id == message_id,
            ReceivedMessage.device_id.in_(
                select(Device.device_id).where(Device.user_id == user_id)
            ),
        )
    )

    return result.rowcount > 0

async def register_publication(
        db: AsyncSession,
        topic: str,
        payload:str,
        qos: int,
        mid: int,
        device_id: str | None = None
) -> Publication:
    publication = Publication(
        device_id=device_id,
        topic=topic,
        payload=payload,
        qos=qos,
        mid=mid,
    )
    db.add(publication)
    await db.flush()

    return publication

async def confirm_publication(
        db: AsyncSession,
        mid: int
) -> None:
    await db.execute(
        update(Publication)
        .where(Publication.mid == mid)
        .values(confirmed_in=datetime.now(timezone.utc))
    )

async def list_publication(
        db: AsyncSession,
        user_id: int,
        limit: int = 20,
        offset: int = 0,
) -> list[Publication]:
    result = await db.execute(
        select(Publication)
        .join(Device, Publication.device_id == Device.device_id)
        .where(Device.user_id == user_id)
        .order_by(Publication.id.desc())
        .limit(limit)
        .offset(offset)
    )

    return list(result.scalars().all())

async def distinct_topics(
        db: AsyncSession,
        user_id: int,
) -> list[str]:
    result = await db.execute(
        select(distinct(ReceivedMessage.topic))
        .join(Device, ReceivedMessage.device_id == Device.device_id)
        .where(Device.user_id == user_id)
        .order_by(ReceivedMessage.topic)
    )

    return [row[0] for row in result.all()]

async def count_messages(
        db: AsyncSession,
        user_id: int,
        topic: str | None = None
) -> int:
    stmt = (
        select(func.count(ReceivedMessage.id))
        .join(Device, ReceivedMessage.device_id == Device.device_id)
        .where(Device.user_id == user_id)
    )

    if topic:
        stmt = stmt.where(ReceivedMessage.topic.like(topic))

    return await db.scalar(stmt) or 0

async def count_publications(db: AsyncSession, user_id: int) -> int:
    stmt = (
        select(func.count(Publication.id))
        .join(Device, Publication.device_id == Device.device_id)
        .where(Device.user_id == user_id)
    )
    return await db.scalar(stmt) or 0