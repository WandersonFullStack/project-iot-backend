from __future__ import annotations
from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update

from app.models.schema_orm import Device, ReceivedMessage, PLC

async def register_device(
        db: AsyncSession,
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
        device_id=device_id, 
        name=name, 
        description=description, 
        topics=topics, 
        api_key_hash=api_key_hash,
    )
    db.add(device)

    await db.flush()
    return device
        
async def search_device(db: AsyncSession, device_id: str) -> Device | None:
    result = await db.execute(select(Device).where(Device.device_id == device_id))

    return result.scalar_one_or_none()
        
async def list_device(
        db:AsyncSession,
        active_only: bool = True,
        limit: int = 50,
        offset: int = 0,
) -> list[Device]:
    stmt = select(Device).order_by(Device.name).limit(limit).offset(offset)

    if active_only:
        stmt = stmt.where(Device.active ==True)

    result = await db.execute(stmt)

    return list(result.scalars().all())
    
async def update_device(
        db: AsyncSession,
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
        update(Device).where(Device.device_id == device_id).values(**values)
    )

    return result.rowcount > 0


async def search_device_plc(db: AsyncSession, device_id: str) -> PLC | None:
    result = await db.execute(
        select(PLC)
        .where(
            PLC.device_id == device_id,
            PLC.active == True 
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

async def renew_api_key(db: AsyncSession, device_id: str, new_hash: str) -> bool:
    result = await db.execute(
        update(Device)
        .where(Device.device_id == device_id, Device.active == True)
        .values(api_key_hash=new_hash)
    )

    return result.rowcount > 0

async def messages_device(
        db: AsyncSession,
        device_id: str,
        limit: int = 20,
        offset: int = 0,
) -> list[ReceivedMessage]:
    result = await db.execute(
        select(ReceivedMessage)
        .where(ReceivedMessage.device_id == device_id)
        .order_by(ReceivedMessage.id.desc())
        .limit(limit)
        .offset(offset)
    )

    return list(result.scalars().all())
