from __future__ import annotations
import csv
import io
import time
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

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