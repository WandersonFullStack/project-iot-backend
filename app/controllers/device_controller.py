from __future__ import annotations
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete

from app.models.schema_orm import Device, ReceivedMessage, PLC

async def register_device(
        db: AsyncSession,
        user_id: int,
        device_id,
        name,
        description,
        topics,
        api_key_hash
) -> Device:
    """
    Persiste um novo dispositivo.
    """
    device = Device(
        user_id=user_id,
        device_id=device_id, 
        name=name, 
        description=description, 
        topics=topics, 
        api_key_hash=api_key_hash,
    )
    db.add(device)

    await db.flush()
    return device
        
async def search_device(
        db: AsyncSession,
        device_id: str,
        user_id: int,
) -> Device | None:
    result = await db.execute(
        select(Device).where(
            Device.device_id == device_id,
            Device.user_id == user_id,
        )
    )

    return result.scalar_one_or_none()


async def search_device_internal(
        db: AsyncSession,
        device_id: str,
) -> Device | None:
    """Lookup for trusted device-authentication and MQTT ingestion paths."""
    result = await db.execute(
        select(Device).where(Device.device_id == device_id)
    )
    return result.scalar_one_or_none()
        
async def list_device(
        db:AsyncSession,
        user_id: int,
        active_only: bool = True,
        limit: int = 50,
        offset: int = 0,
) -> list[Device]:
    stmt = (
        select(Device)
        .where(Device.user_id == user_id)
        .order_by(Device.name)
        .limit(limit)
        .offset(offset)
    )

    if active_only:
        stmt = stmt.where(Device.active ==True)

    result = await db.execute(stmt)

    return list(result.scalars().all())


async def list_device_ids(db: AsyncSession, user_id: int) -> set[str]:
    result = await db.execute(
        select(Device.device_id).where(
            Device.user_id == user_id,
            Device.active == True,
        )
    )
    return set(result.scalars().all())


async def search_device_by_topic(
        db: AsyncSession,
        topic: str,
        user_id: int,
) -> Device | None:
    """Resolve an exact publish topic within the authenticated user's devices."""
    result = await db.execute(
        select(Device).where(
            Device.user_id == user_id,
            Device.active == True,
            Device.topics.any(topic),
        )
    )
    return result.scalars().first()
    
async def update_device(
        db: AsyncSession,
        user_id: int,
        device_id: str,
        name: str | None = None,
        description: str | None = None,
        topics: list[str] | None = None,
        active: bool | None = None
) -> bool:
    """Atualiza apenas os campos fornecidos (PATH semântico)."""
    values: dict = {}
    if name is not None: values["name"] = name
    if description is not None: values["description"] = description
    if topics is not None: values["topics"] = topics
    if active is not None: values["active"] = active
    if not values:
        return False
    
    result = await db.execute(
        update(Device)
        .where(
            Device.device_id == device_id,
            Device.user_id == user_id,
        )
        .values(**values)
    )

    return result.rowcount > 0

async def delete_device(
    db: AsyncSession,
    device_id: str,
    user_id: int,
) -> bool:
    """
    Deleção fisica do dispositivo. O ON DELETE CASCADE remove em sequencia
    os PLCs, os registradores desses PLCs, e o histórico de mensagens e
    publicações vinculado ao device_id.

    Para desativar sem perder o histórico, use update_device(active=False).
    """
    result = await db.execute(
        delete(Device).where(
            Device.device_id == device_id,
            Device.user_id == user_id,
        )
    )

    return result.rowcount > 0

async def search_device_plc(
        db: AsyncSession,
        device_id: str,
        user_id: int,
) -> PLC | None:
    result = await db.execute(
        select(PLC)
        .join(Device, PLC.device_id == Device.device_id)
        .where(
            PLC.device_id == device_id,
            PLC.active == True,
            Device.user_id == user_id,
        )
    )
    plc = result.scalar_one_or_none()

    return plc
        
async def update_status(db: AsyncSession, device_id: str, status: str) -> None:
    """
    Chamado pelo on_massage toda vez que uma mensagem é recebida
    de um dispositivo identificado.
    """
    await db.execute(
        update(Device)
        .where(Device.device_id == device_id, Device.active == True)
        .values(status=status, last_contact=datetime.now(timezone.utc))
    )

async def update_offline_devices(db: AsyncSession, timeout: int = 5) -> None:
    """Chamada pela tarefa de background para marcar dispositivos sem contato como offline."""

    limit = datetime.now(timezone.utc) - timedelta(minutes=timeout)
    
    await db.execute(
        update(Device).where(
            Device.status == "online",
            Device.active == True,
            (Device.last_contact == None) | (Device.last_contact < limit),
        ).values(status="offline")
    )

async def renew_api_key(
        db: AsyncSession,
        device_id: str,
        user_id: int,
        new_hash: str,
) -> bool:
    result = await db.execute(
        update(Device)
        .where(
            Device.device_id == device_id,
            Device.user_id == user_id,
            Device.active == True,
        )
        .values(api_key_hash=new_hash)
    )

    return result.rowcount > 0

async def messages_device(
        db: AsyncSession,
        device_id: str,
        user_id: int,
        limit: int = 20,
        offset: int = 0,
) -> list[ReceivedMessage]:
    result = await db.execute(
        select(ReceivedMessage)
        .join(Device, ReceivedMessage.device_id == Device.device_id)
        .where(
            ReceivedMessage.device_id == device_id,
            Device.user_id == user_id,
        )
        .order_by(ReceivedMessage.id.desc())
        .limit(limit)
        .offset(offset)
    )

    return list(result.scalars().all())
