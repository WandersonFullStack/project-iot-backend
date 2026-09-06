from __future__ import annotations

from sqlalchemy import select, update, delete, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema_orm import Device, PLC, MapRegister

# -> HELPER INTERNO
async def _attach_total_registers(
        db: AsyncSession,
        plc: PLC
) -> None:
    count = await db.scalar(
        select(func.count(MapRegister.id))
        .where(MapRegister.plc_id == plc.id, MapRegister.active == True)
    )

    plc.total_registers = count or 0    # type: ignore[attr-defined]

# -> PLC
async def create_plc(
        db: AsyncSession,
        device_id: str,
        name: str,
        ip: str,
        port_modbus: int = 502,
        port_tcp: int = 9000,
        protocol: str = "modbus",
        description: str | None = None,
        unit_id: int = 255,
        timeout: float = 5.0,
) -> PLC:
    plc = PLC(
        device_id=device_id,
        name=name,
        description=description,
        ip=ip,
        port_modbus=port_modbus,
        port_tcp=port_tcp,
        protocol=protocol,
        unit_id=unit_id,
        timeout=timeout,
    )

    db.add(plc)
    await db.flush()
    await _attach_total_registers(db, plc)

    return plc

async def list_plcs(
        db: AsyncSession,
        user_id: int,
        active_only: bool = True,
        limit: int = 50,
        offset: int = 0,
) -> list[PLC]:
    stmt = (
        select(PLC)
        .join(Device, PLC.device_id == Device.device_id)
        .where(Device.user_id == user_id)
        .order_by(PLC.name)
        .limit(limit)
        .offset(offset)
    )

    if active_only:
        stmt = stmt.where(PLC.active == True)

    result = await db.execute(stmt)
    plcs = list(result.scalars().all())

    for plc in plcs:
        await _attach_total_registers(db, plc)

    return plcs

async def search_plc(
        db: AsyncSession,
        plc_id: int,
        user_id: int,
) -> PLC | None:
    result = await db.execute(
        select(PLC)
        .join(Device, PLC.device_id == Device.device_id)
        .where(
            PLC.id == plc_id,
            Device.user_id == user_id,
        )
    )
    plc = result.scalar_one_or_none()

    if plc:
        await _attach_total_registers(db, plc)

    return plc


async def search_plc_internal(
        db: AsyncSession,
        plc_id: int,
) -> PLC | None:
    """Lookup reserved for trusted industrial-protocol paths."""
    result = await db.execute(select(PLC).where(PLC.id == plc_id))
    plc = result.scalar_one_or_none()
    if plc:
        await _attach_total_registers(db, plc)
    return plc


async def search_plc_by_device_id(
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

    if plc:
        await _attach_total_registers(db, plc)

    return plc

async def update_plc(
        db: AsyncSession,
        plc_id: int,
        user_id: int,
        **fields
) -> bool:
    allowed = {"name", "description", "ip", "port_modbus", "port_tcp",
               "protocol", "unit_id", "timeout", "active"}
    
    values = {k: v for k, v in fields.items() if k in allowed and v is not None}

    if not values:
        return False
    
    result = await db.execute(
        update(PLC)
        .where(
            PLC.id == plc_id,
            PLC.device_id.in_(
                select(Device.device_id).where(Device.user_id == user_id)
            ),
        )
        .values(**values)
    )

    return result.rowcount > 0

async def delete_plc(
    db: AsyncSession,
    plc_id: int,
    user_id: int,
) -> bool:
    """
    Deleção fisica do PLC. OS registradores são removidos pelo
    ON DELETE CASCADE de map_registers.plc_id -> plc.id.

    Para suspender sem perder o mapeamento, use update_plc(active=False).
    """
    result = await db.execute(
        delete(PLC)
        .where(
            PLC.id == plc_id,
            PLC.device_id.in_(
                select(Device.device_id).where(Device.user_id == user_id)
            ),
        )
    )

    return result.rowcount > 0

# -> REGISTERS
async def create_register(
        db:AsyncSession,
        plc_id: int,
        type: str,
        tag_name: str,
        address: int,
        topic: str,
        description: str | None = None,
        unit: str | None = None,
        scale: float = 1.0,
        offset: float = 0.0,
        qos: int = 1,
        read_only: bool = True
) -> int:
    """Retorna o id do registro criado ou -1 se (plc_id, type, address) já existir."""

    stmt = (
        pg_insert(MapRegister)
        .values(
            plc_id=plc_id, type=type, tag_name=tag_name, address=address, topic=topic,
            description=description, unit=unit, scale=scale,
            offset=offset, qos=qos, read_only=read_only,
        )
        .on_conflict_do_nothing(constraint="uq_register_plc_type_address")
        .returning(MapRegister.id)
    )

    result = await db.execute(stmt)
    row = result.fetchone()

    return row[0] if row else -1

async def create_register_bulk(
        db: AsyncSession,
        plc_id: int,
        items: list[dict],
) -> int:
    """
    INSERT OR REPLACE via on_conflict_do_update.
    Toda a operação roda em uma transação única.
    """

    if not items:
        return 0
    
    stmt = pg_insert(MapRegister).values([
        {
            "plc_id": plc_id,
            "type": item["type"],
            "tag_name": item["tag_name"],
            "address": item["address"],
            "topic": item["topic"],
            "description": item.get("description"),
            "unit": item.get("unit"),
            "scale": item.get("scale", 1.0),
            "offset": item.get("offset", 0.0),
            "qos": item.get("qos", 1),
            "read_only": item.get("read_only", True),
        }
        for item in items
    ])

    stmt = stmt.on_conflict_do_update(
        constraint="uq_register_plc_type_address",
        set_={
            "tag_name": stmt.excluded.tag_name,
            "topic": stmt.excluded.topic,
            "description": stmt.excluded.description,
            "unit": stmt.excluded.unit,
            "scale": stmt.excluded.scale,
            "offset": stmt.excluded.offset,
            "qos": stmt.excluded.qos,
            "read_only": stmt.excluded.read_only,
        },
    )

    result = await db.execute(stmt)
    return result.rowcount

async def list_registers(
        db: AsyncSession,
        plc_id: int,
        type: str | None = None,
        active_only: bool = True,
        limit: int = 200,
        offset: int = 0,
) -> list[MapRegister]:
    stmt = (
        select(MapRegister)
        .where(MapRegister.plc_id == plc_id)
        .order_by(MapRegister.type, MapRegister.address)
        .limit(limit)
        .offset(offset)
    )

    if type:
        stmt = stmt.where(MapRegister.type == type)
    if active_only:
        stmt = stmt.where(MapRegister.active == True)

    result = await db.execute(stmt)
    return list(result.scalars().all())

async def search_register(
        db: AsyncSession,
        plc_id: int,
        register_id: int,
) -> MapRegister | None:
    result = await db.execute(
        select(MapRegister)
        .where(
            MapRegister.id == register_id,
            MapRegister.plc_id == plc_id
        )
    )

    return result.scalar_one_or_none()

async def update_register(
        db: AsyncSession,
        register_id: int,
        plc_id: int,
        **fields,
) -> bool:
    allowed = {"tag_name", "topic", "description", "unit", "scale",
               "offset", "qos", "read_only", "active"}
    
    values = {k: v for k, v in fields.items() if k in allowed}
    if not values:
        return False
    
    result = await db.execute(
        update(MapRegister)
        .where(MapRegister.id == register_id, MapRegister.plc_id == plc_id)
        .values(**values)
    )

    return result.rowcount > 0

async def delete_register(
        db: AsyncSession,
        register_id: int,
        plc_id: int,
) -> bool:
    result = await db.execute(
        delete(MapRegister)
        .where(
            MapRegister.id == register_id,
            MapRegister.plc_id == plc_id,
        )
    )

    return result.rowcount > 0

async def delete_all_registers(
    db: AsyncSession,
    plc_id: int,
) -> int:
    """
    Limpa o mapa de um PLC sem remover o mesmo.
    Retorna o tatal removido.
    """
    result = await db.execute(
        delete(MapRegister).where(MapRegister.plc_id == plc_id)
    )

    return result.rowcount

async def load_map_modbus(
        db: AsyncSession
) -> list[dict]:
    """
    Carrega o mapa completo para inicializar o ModbusGateway.
    Faz join com PLCs para trazer device_id, ip, unit_id e timeout.
    Retorna apenas registradores e PLCs ativos.
    """
    result = await db.execute(
        select(
            MapRegister.address,
            MapRegister.topic,
            MapRegister.unit,
            MapRegister.scale,
            PLC.device_id,
            PLC.ip,
            PLC.unit_id,
            PLC.timeout,
        )
        .join(PLC, MapRegister.plc_id == PLC.id)
        .where(MapRegister.active == True, PLC.active == True)
        .order_by(PLC.id, MapRegister.type, MapRegister.address)
    )

    return [row._asdict() for row in result.all()]

