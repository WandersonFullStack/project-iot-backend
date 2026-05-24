from __future__ import annotations
import csv
import io
import time
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status, BackgroundTasks

from app.controllers.database_controller import DatabaseController
from app.models.schemas import (
    PLCIn, PLCOut, PLCUpdate, MapRegisterIn,
    MapRegisterOut, MapRegisterUpdate, MapBulkIn,
    TestConnectionOut, PagesParams
)
from app.config.broker_configs import log
from app.config.modbus_configs import OFFSET_MODBUS as offset_mb

router = APIRouter(prefix="/api/v1/plcs", tags=["PLCs"])

# == DEPENDÊNCIAS ==========================================================================

def get_db() -> DatabaseController:
    from app.main.main import db
    return db

DB = Annotated[DatabaseController, Depends(get_db)]

def _page_params(
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0)
) -> PagesParams:
    return PagesParams(limit=limit, offset=offset)

Pag = Annotated[PagesParams, Depends(_page_params)]

# == HELPERS ===============================================================================

def _plc_or_404(db: DatabaseController, plc_id: int) -> dict:
    row = db.search_plc(plc_id)
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"PLC id={plc_id} not found"
        )
    return dict(row)

def _row_for_plc(row) -> dict:
    d = dict(row)
    d["active"] = bool(d.get("active", 1))
    d["total_registers"] = d.get("total_registers", 0)
    return d

def _row_for_register(row) -> dict:
    """Calcula o endereço de display no formato padrão."""
    d = dict(row)
    offset = offset_mb.get(d.get("type", "holding"), 40001)
    d["address_modbus"] = d["address"] + offset
    d["active"] = bool(d.get("active", 1))
    d["read_only"] = bool(d.get("read_only", 1))
    return d

# == CRUD OF PLCs ==========================================================================

@router.post(
    "",
    response_model=PLCOut,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new PLC on the gateway."
)
def create_plc(body: PLCIn, db: DB):
    """
    Vincula um CLP a um dispositivo já registrado via `device_id`.
    
    O `device_id`deve existir na tabela `devices` -> o CLP não é 
    um conceito autônomo, mas uma especialização de dispositivo com
    informações de conectividade de rede industrial (ip, port, unit_id).

    Um mesmo `device_id` pode ter apenas um CLP associado.
    """
    # Verifica se o dispositivo existe
    dev = db.search_device(body.device_id)
    if not dev:
        raise HTTPException(
            status_code=422,
            detail=f"Device '{body.device_id}' not found. Register the device first."
        )
    
    plc_id = db.create_plc(
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
    return _row_for_plc(db.search_plc(plc_id))

@router.get(
    "",
    response_model=list[PLCOut],
    summary="List of registered PLCs with register count"
)
def list_plcs(
    db: DB,
    pag: Pag,
    active_only: bool = Query(default=True)
):
    return [_row_for_plc(r) for r in db.list_plcs(active_only, pag.limit, pag.offset)]

@router.get("/{plc_id}", response_model=PLCOut, summary="Search for a PLC by ID")
def search_plc(plc_id: int, db: DB):
    return _row_for_plc(_plc_or_404(db, plc_id))

@router.patch("/{plc_id}", response_model=PLCOut, summary="Update PLC fields")
def update_plc(plc_id: int, body: PLCUpdate, db: DB):
    """
    Aceita qualquer subconjunto dos campos -> apenas os não-None são gravados.
    Para desativar sem excluir: `{"active": false}`.
    Desativar o CLP não apaga os registradores, apenas suspende
    o carregamento do mapa no ModbusGateway.
    """
    _plc_or_404(db, plc_id)
    db.update_plc(
        plc_id,
        **{k: v for k, v in body.model_dump().items() if v is not None}
    )
    return _row_for_plc(db.search_plc(plc_id))

@router.delete(
    "/{plc_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Deactive PLC (soft delete)"
)
def remove_plc(plc_id: int, db: DB):
    """
    Não apaga o registro -> apenas seta àctive=0`.
    Os registradores permanecem no banco para histórico.
    Use PATCH com `{"active": true}` para reativar.
    """
    _plc_or_404(db, plc_id)
    db.update_plc(plc_id, active=False)

# == TESTE DE CONEXÃO ====================================================================

@router.post(
    "/{plc_id}/test",
    response_model=TestConnectionOut,
    summary="Test the connection TCP/Modbus com o PLC"
)
def test_connection(plc_id: int, db: DB):
    """
    Abre uma conexão Modbus TCP com o PLC, lê o registrador holding 0
    e fecha a conexão.

    Roda em thread pool (rota síncrona) -> não bloqueia o event loop
    do FastAPI durante o timeout de conexão TCP.
    """
    plc = _plc_or_404(db, plc_id)

    try:
        from pymodbus.client import ModbusTcpClient
    except ImportError:
        raise HTTPException(
            status_code=501,
            detail="pymodbus is not installed"
        )
    
    inicio = time.perf_counter()
    client = ModbusTcpClient(
        host=plc["ip"],
        port=plc["port_modbus"],
        timeout=plc["timeout"]
    )

    try:
        connected = client.connect()
        if not connected:
            return TestConnectionOut(
                success=False, message="Failed to establish TCP connection.",
                ip=plc["ip"], port=plc["port_modbus"], unit_id=plc["unit_id"],
                time_ms=round((time.perf_counter() - inicio) * 1000, 1),
            )
        
        # Lê o primeiro holding register (FC 03) para validar comunicação
        result = client.read_holding_registers(
            address=0, count=1, device_id=plc["unit_id"]
        )
        elapsed = round((time.perf_counter() - inicio) * 1000, 1)

        if result.isError():
            return TestConnectionOut(
                success=False,
                message=f"Connection OK, but reading failed: {result}",
                ip=plc["ip"], port=plc["port_modbus"],
                unit_id=plc["unit_id"], time_ms=elapsed,
            )
        
        return TestConnectionOut(
            success=True,
            message="Successful Modbus connection and reading.",
            ip=plc["ip"], port=plc["port_modbus"],
            unit_id=plc["unit_id"], time_ms=elapsed,
            value_reg0=result.registers[0],
        )
    
    except Exception as e:
        return TestConnectionOut(
            success=False, message=str(e),
            ip=plc["ip"], port=plc["port_modbus"], unit_id=plc["unit_id"],
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
def bulk_create_registers(plc_id: int, body: MapBulkIn, db: DB, bg: BackgroundTasks):
    """
    Usa INSERT OR REPLACE — se o par (clp_id, tipo, endereco) já existir,
    os demais campos são atualizados. Ideal para importar um mapeamento
    exportado de uma ferramenta de engenharia (TIA Portal, Unity Pro, etc.)
    ou de uma planilha de I/O do projeto.

    Toda a operação roda em uma única transação: ou todos os registradores
    são inseridos ou nenhum é (falha atômica).
    """
    _plc_or_404(db, plc_id)
    items = [r.model_dump() for r in body.registers]
    total = db.create_registers_bulk(plc_id, items)
    bg.add_task(_reload)
    return {"plc_id": plc_id, "inserted_or_updated": total}

@router.get(
    "/{plc_id}/registers/export",
    summary="Exports PLC registers in CSV format.",
    response_class=Response
)
def export_registers_csv(
    plc_id: int,
    db: DB,
    active_only: bool = Query(default=True),
):
    """
    Gera um CSV com todos os registradores do CLP.
    O arquivo pode ser importado de volta via POST /bulk após edição manual.
    """
    _plc_or_404(db, plc_id)
    rows = db.list_registers(plc_id, active_only=active_only)

    fields = [
        "type", "address", "address_modbus", "topic", "unit",
        "scale", "offset", "qos", "read_only", "description",
    ]

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({c: _row_for_register(row).get(c, "") for c in fields})

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
def list_registers(
    plc_id: int,
    db: DB,
    pag: Pag,
    type: Optional[str] = Query(
        default=None,
        description="Filter by type: holding | coil | input | discrete",
    ),
    active_only: bool = Query(default=True),
):
    _plc_or_404(db, plc_id)
    rows = db.list_registers(plc_id, type=type, active_only=active_only,
                             limit=pag.limit, offset=pag.offset)
    return [_row_for_register(r) for r in rows]

@router.post(
    "/{plc_id}/registers",
    response_model=MapRegisterOut,
    status_code=status.HTTP_201_CREATED,
    summary="Adds a register to the PLC map."
)
def create_registers(plc_id: int, body: MapRegisterIn, db: DB, bg: BackgroundTasks):
    """
    Adiciona um único mapeamento. Para importação em masa, use /bulk.

    Retorna 409 Conflict se o par (type, address) já existir para este CLP.
    Use PATCH /{register_id} para atualizar um registrador existente.
    """
    _plc_or_404(db, plc_id)
    register_id = db.create_register(
        plc_id=plc_id,
        type=body.type.value,
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
    bg.add_task(_reload)
    return _row_for_register(db.search_register(plc_id, register_id))

@router.get(
    "/{plc_id}/registers/{register_id}",
    response_model=MapRegisterOut,
    summary="Search a register by ID"
)
def search_register(plc_id: int, register_id: int, db: DB):
    row = db.search_register(plc_id, register_id)
    if not row:
        raise HTTPException(
            status_code=404,
            detail="Register not found."
        )
    return _row_for_register(row)

@router.patch(
    "/{plc_id}/registers/{register_id}",
    response_model=MapRegisterOut,
    summary="Updates fields in a register."
)
def update_register(plc_id: int, register_id: int, body: MapRegisterUpdate, db: DB, bg: BackgroundTasks):
    """
    Apenas tópico, descrição, unidade, escala, offset, qos e ativo
    são atualizáveis. O tipo e o endereço são imutáveis após a criação
    pois identificam unicamente o registrador no protocolo Modbus.
    Para trocar tipo/endereço, delete e recrie.
    """
    if not db.search_register(plc_id, register_id):
        raise HTTPException(
            status_code=404,
            detail="Register not found."
        )
    db.update_register(
        register_id, plc_id,
        **{k: v for k, v in body.model_dump().items() if v is not None},
    )
    bg.add_task(_reload)
    return _row_for_register(db.search_register(plc_id, register_id))

@router.delete(
    "/{plc_id}/registers/{register_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Permanently remove a register."
)
def delete_register(plc_id: int, register_id: int, db: DB, bg: BackgroundTasks):
    """Deleção física -> registrador não têm histórico associado."""

    if not db.delete_register(register_id, plc_id):
        raise HTTPException(
            status_code=404,
            detail="Register not found."
        )
    
    bg.add_task(_reload)
    
def _reload():
    """Wrapper síncrono chamado pelo BackgroundTasks."""

    import asyncio
    from app.main.main import reload_map_modbus
    
    asyncio.run(reload_map_modbus())