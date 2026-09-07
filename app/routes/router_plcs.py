from __future__ import annotations
import csv
import io
import time
import asyncio
from typing import Annotated, Optional, AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import AsyncSessionLocal
from app.controllers import plc_controller as pc, device_controller as dc
from app.models.schemas import (
    PLCIn, PLCOut, PLCUpdate, MapRegisterIn,
    MapRegisterOut, MapRegisterUpdate, MapBulkIn,
    TestConnectionOut, PLCStatusOut, PagesParams
)
from app.services.gateway_runtime import reload_map_modbus
from app.services.plc_health import check_plc
from app.config.broker_configs import log
from app.config.modbus_configs import _OFFSET_MODBUS as offset_mb
from app.auth.dependencies.depends import CurrentUser

router = APIRouter(prefix="/api/v1/plcs", tags=["PLCs"])

# == DEPENDÊNCIAS ==========================================================================

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

DB = Annotated[AsyncSession, Depends(get_db)]

async def _page_params(
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0)
) -> PagesParams:
    return PagesParams(limit=limit, offset=offset)

Pag = Annotated[PagesParams, Depends(_page_params)]

# == HELPERS ===============================================================================

async def _plc_or_404(db: DB, plc_id: int, user_id: int):
    device = await pc.search_plc(db, plc_id, user_id)
    if not device:
        raise HTTPException(
            status_code=404,
            detail=f"PLC id={plc_id} not found"
        )
    return device


# == CRUD OF PLCs ==========================================================================

@router.post(
    "",
    response_model=PLCOut,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new PLC on the gateway."
)
async def create_plc(body: PLCIn, db: DB, user: CurrentUser):
    """
    Vincula um CLP a um dispositivo já registrado via `device_id`.
    
    O `device_id`deve existir na tabela `devices` -> o CLP não é 
    um conceito autônomo, mas uma especialização de dispositivo com
    informações de conectividade de rede industrial (ip, port, unit_id).

    Um mesmo `device_id` pode ter apenas um CLP associado.
    """
    # Verifica se o dispositivo existe
    device = await dc.search_device(db, body.device_id, user.id)
    if not device:
        raise HTTPException(
            status_code=422,
            detail=f"Device '{body.device_id}' not found. Register the device first."
        )
    
    plc_id = await pc.create_plc(
        db,
        device_id=body.device_id,
        name=body.name,
        ip=body.ip,
        port_modbus=body.port_modbus,
        port_tcp=body.port_tcp,
        protocol=body.protocol,
        description=body.description,
        unit_id=body.unit_id,
        timeout=body.timeout
    )

    await db.commit()
    return plc_id

@router.get(
    "",
    response_model=list[PLCOut],
    summary="List of registered PLCs with register count"
)
async def list_plcs(
    db: DB,
    pag: Pag,
    user: CurrentUser,
    active_only: bool = Query(default=True),
):
    return await pc.list_plcs(
        db, user.id, active_only, pag.limit, pag.offset
    )

@router.get(
    "/status",
    response_model=list[PLCStatusOut],
    summary="Connection status of every registered PLC",
)
async def status_all_plcs(
    db: DB,
    pag: Pag,
    user: CurrentUser,
    deep: bool = Query(
        default=False, 
        description="Also read holding 0 to confirme the slave answers"
    ),
):
    plcs = await pc.list_plcs(db, user.id, True, pag.limit, pag.offset)

    return await asyncio.gather(*(check_plc(plc, deep) for plc in plcs))

@router.get(
    "/{plc_id}/status",
    response_model=PLCStatusOut,
    summary="check whether a PLC is reachable",
)
async def status_plc(
    plc_id: int,
    db: DB,
    user: CurrentUser,
    deep: bool = Query(default=False),
    force: bool = Query(
        default=False, description="Ignore the cached result"
    ),
):
    plc = await _plc_or_404(db, plc_id, user.id)

    return await check_plc(plc, deep=deep, force=force)

@router.get(
        "/{plc_id}", 
        response_model=PLCOut, 
        summary="Search for a PLC by ID"
)
async def search_plc(plc_id: int, db: DB, user: CurrentUser):
    return await _plc_or_404(db, plc_id, user.id)


@router.patch(
        "/{plc_id}", 
        response_model=PLCOut, 
        summary="Update PLC fields"
)
async def update_plc(plc_id: int, body: PLCUpdate, db: DB, user: CurrentUser):
    """
    Aceita qualquer subconjunto dos campos -> apenas os não-None são gravados.
    Para desativar sem excluir: `{"active": false}`.
    Desativar o CLP não apaga os registradores, apenas suspende
    o carregamento do mapa no ModbusGateway.
    """
    await _plc_or_404(db, plc_id, user.id)
    await pc.update_plc(
        db,
        plc_id,
        user.id,
        **{k: v for k, v in body.model_dump().items() if v is not None}
    )
    await db.commit()
    
    return await pc.search_plc(db, plc_id, user.id)

@router.delete(
    "/{plc_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Deactive or permanently delete a PLC"
)
async def remove_plc(
    plc_id: int, 
    db: DB, 
    bg: BackgroundTasks,
    user: CurrentUser,
    permanent: bool = Query(
        default=False,
        description="If true, deletes the PLC and all of its registers.",
    ),
):
    """
    Por padrão apenas seta àctive=false` -> os
    registradores permanecem no banco para histórico e
    o mapa deixa de ser carregado no ModbusGateway.
    Use PATCH com `{"active": true}` para reativar.

    Com `?permanent=true`a linha é removida do banco e o ON DELETE CASCADE
    apaga todos os registradores do PLC. Operação irreversivel.
    """
    await _plc_or_404(db, plc_id, user.id)

    if permanent:
        await pc.delete_plc(db, plc_id, user.id)
    else:
        await pc.update_plc(db, plc_id, user.id, active=False)
    
    await db.commit()
    bg.add_task(reload_map_modbus)

# == TESTE DE CONEXÃO ====================================================================

@router.post(
    "/{plc_id}/test",
    response_model=TestConnectionOut,
    summary="Test the connection TCP/Modbus com o PLC"
)
async def test_connection(plc_id: int, db: DB, user: CurrentUser):
    """
    Abre uma conexão Modbus TCP com o PLC, lê o registrador holding 0
    e fecha a conexão.

    Roda em thread pool (rota síncrona) -> não bloqueia o event loop
    do FastAPI durante o timeout de conexão TCP.
    """
    plc = await _plc_or_404(db, plc_id, user.id)

    try:
        from pymodbus.client import ModbusTcpClient
    except ImportError:
        raise HTTPException(
            status_code=501,
            detail="pymodbus is not installed"
        )
    
    inicio = time.perf_counter()
    client = ModbusTcpClient(
        host=plc.ip,
        port=plc.port_modbus,
        timeout=plc.timeout
    )

    try:
        connected = client.connect()
        if not connected:
            return TestConnectionOut(
                success=False, 
                message="Failed to establish TCP connection.",
                ip=plc.ip, 
                port=plc.port_modbus, 
                unit_id=plc.unit_id,
                time_ms=round((time.perf_counter() - inicio) * 1000, 1),
            )
        
        # Lê o primeiro holding register (FC 03) para validar comunicação
        result = client.read_holding_registers(
            address=0, 
            count=1, 
            device_id=plc.unit_id
        )
        elapsed = round((time.perf_counter() - inicio) * 1000, 1)

        if result.isError():
            return TestConnectionOut(
                success=False,
                message=f"Connection OK, but reading failed: {result}",
                ip=plc.ip, 
                port=plc.port_modbus,
                unit_id=plc.unit_id, 
                time_ms=elapsed,
            )
        
        return TestConnectionOut(
            success=True,
            message="Successful Modbus connection and reading.",
            ip=plc.ip, 
            port=plc.port_modbus,
            unit_id=plc.unit_id, 
            time_ms=elapsed,
            value_reg0=result.registers[0],
        )
    
    except Exception as e:
        return TestConnectionOut(
            success=False, message=str(e),
            ip=plc.ip, 
            port=plc.port_modbus, 
            unit_id=plc.unit_id,
            time_ms=round((time.perf_counter() - inicio) * 1000, 1),
        )
    finally:
        client.close()

# == CRUD DE REGISTRADORES ==============================================================

@router.post(
    "/{plc_id}/registers/bulk",
    status_code=status.HTTP_201_CREATED,
    summary="Imports multiple registers at once."
)
async def bulk_create_registers(plc_id: int, body: MapBulkIn, db: DB, bg: BackgroundTasks, user: CurrentUser):
    """
    Usa INSERT OR REPLACE — se o par (clp_id, tipo, endereco) já existir,
    os demais campos são atualizados. Ideal para importar um mapeamento
    exportado de uma ferramenta de engenharia (TIA Portal, Unity Pro, etc.)
    ou de uma planilha de I/O do projeto.

    Toda a operação roda em uma única transação: ou todos os registradores
    são inseridos ou nenhum é (falha atômica).
    """
    await _plc_or_404(db, plc_id, user.id)
    items = [r.model_dump() for r in body.registers]
    total = await pc.create_register_bulk(db, plc_id, items)

    await db.commit()
    bg.add_task(reload_map_modbus)
    
    return {"plc_id": plc_id, "inserted_or_updated": total}

@router.get(
    "/{plc_id}/registers/export",
    summary="Exports PLC registers in CSV format.",
    response_class=Response
)
async def export_registers_csv(
    plc_id: int,
    db: DB,
    user: CurrentUser,
    active_only: bool = Query(default=True),
):
    """
    Gera um CSV com todos os registradores do CLP.
    O arquivo pode ser importado de volta via POST /bulk após edição manual.
    """
    await _plc_or_404(db, plc_id, user.id)
    rows = await pc.list_registers(db, plc_id, active_only=active_only)

    fields = [
        "type", "tag_name", "address", "address_modbus", "topic", "unit",
        "scale", "offset", "qos", "read_only", "description",
    ]

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({c: getattr(row, c, "") for c in fields})

    return Response(
        content=buffer.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename=plc_{plc_id}_registers.csv"}
    )

@router.get(
    "/{plc_id}/registers",
    response_model=list[MapRegisterOut],
    summary="Lists the mapped registers of a PLC."
)
async def list_registers(
    plc_id: int,
    db: DB,
    pag: Pag,
    user: CurrentUser,
    type: Optional[str] = Query(
        default=None,
        description="Filter by type: holding | coil | input | discrete",
    ),
    active_only: bool = Query(default=True),
):
    await _plc_or_404(db, plc_id, user.id)
    rows = await pc.list_registers(db, plc_id, type=type, active_only=active_only,
                             limit=pag.limit, offset=pag.offset)
    return rows

@router.post(
    "/{plc_id}/registers",
    response_model=MapRegisterOut,
    status_code=status.HTTP_201_CREATED,
    summary="Adds a register to the PLC map."
)
async def create_registers(plc_id: int, body: MapRegisterIn, db: DB, bg: BackgroundTasks, user: CurrentUser):
    """
    Adiciona um único mapeamento. Para importação em masa, use /bulk.

    Retorna 409 Conflict se o par (type, address) já existir para este CLP.
    Use PATCH /{register_id} para atualizar um registrador existente.
    """
    await _plc_or_404(db, plc_id, user.id)
    register_id = await pc.create_register(
        db,
        plc_id=plc_id,
        type=body.type.value,
        tag_name=body.tag_name,
        address=body.address,
        topic=body.topic,
        description=body.description,
        unit=body.unit,
        scale=body.scale,
        offset=body.offset,
        qos=body.qos,
        read_only=body.read_only
    )
    
    if register_id == -1:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Register {body.type.value} address {body.address} There is already a patch for this PLC. Use the PATCH to update it."
        )
    await db.commit()
    bg.add_task(reload_map_modbus)

    return await pc.search_register(db, plc_id, register_id)

@router.get(
    "/{plc_id}/registers/{register_id}",
    response_model=MapRegisterOut,
    summary="Search a register by ID"
)
async def search_register(
    plc_id: int,
    register_id: int,
    db: DB,
    user: CurrentUser,
):
    await _plc_or_404(db, plc_id, user.id)
    row = await pc.search_register(db, plc_id, register_id)
    
    if not row:
        raise HTTPException(
            status_code=404,
            detail="Register not found."
        )
    
    return row

@router.patch(
    "/{plc_id}/registers/{register_id}",
    response_model=MapRegisterOut,
    summary="Updates fields in a register."
)
async def update_register(
    plc_id: int, 
    register_id: int, 
    body: MapRegisterUpdate, 
    db: DB, 
    bg: BackgroundTasks,
    user: CurrentUser
):
    """
    Apenas tag_name, tópico, descrição, unidade, escala, offset, qos e ativo
    são atualizáveis. O tipo e o endereço são imutáveis após a criação
    pois identificam unicamente o registrador no protocolo Modbus.
    Para trocar tipo/endereço, delete e recrie.
    """
    await _plc_or_404(db, plc_id, user.id)
    register = await pc.search_register(db, plc_id, register_id)

    if not register:
        raise HTTPException(
            status_code=404,
            detail="Register not found."
        )
    
    fields = body.model_dump(exclude_unset=True)

    await pc.update_register(
        db,
        register_id, 
        plc_id,
        **fields,
    )
    await db.commit()
    bg.add_task(reload_map_modbus)

    return await pc.search_register(db, plc_id, register_id)

@router.delete(
    "/{plc_id}/registers",
    summary="Remove every registers from a PLC map."
)
async def delete_all_registers(
    plc_id: int,
    db: DB,
    bg: BackgroundTasks,
    user: CurrentUser,
):
    """
    Limpa o mapeamento inteiro mantendo o PLC cadastrado.
    Util antes de reimportar um mapa via POST /bulk.
    """
    await _plc_or_404(db, plc_id, user_id)
    total = await pc.delete_all_registers(db, plc_id)

    await db.commit()
    bg.add_task(reload_map_modbus)

    return {"plc_id": plc_id, "deleted": total}

@router.delete(
    "/{plc_id}/registers/{register_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Permanently remove a register."
)
async def delete_register(
    plc_id: int,
    register_id: int,
    db: DB,
    bg: BackgroundTasks,
    user: CurrentUser,
):
    """Deleção física -> registrador não têm histórico associado."""

    await _plc_or_404(db, plc_id, user.id)
    register = await pc.delete_register(db, register_id, plc_id)

    if not register:
        raise HTTPException(
            status_code=404,
            detail="Register not found."
        )
    
    await db.commit()
    bg.add_task(reload_map_modbus)
    