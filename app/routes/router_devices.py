from __future__ import annotations
import uuid
import json
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.controllers.database_controller import DatabaseController
from app.models.schemas import (
    DeviceIn, DeviceUpdate, DeviceOut,
    DeviceRegisterOut, RenewalKeyOut,
    MessageOut, PagesParams
)
from app.auth.device_auth import generate_api_key, hash_api_key
from app.auth.dependencies.depends import CurrentUser, OperatorUser, AdminUser

router = APIRouter(prefix="/api/v1/devices", tags=["Devices"])

def get_db() -> DatabaseController:
    from app.main.main import db    # importação circular evitada via lazy import
    return db

DB = Annotated[DatabaseController, Depends(get_db)]

def _page_params(
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0)
) -> PagesParams:
    return PagesParams(limit=limit, offset=offset)

Pag = Annotated[PagesParams, Depends(_page_params)]

def _row_to_devices(row) -> dict:
    """
    Converte sqlite3.Row para dict deserializando o campo `topics`.
    Necessário porque o SQLite armazena listas como strings JSON.
    """
    d = dict(row)
    d["topics"] = json.loads(d.get("topics") or "[]")
    d["active"] = bool(d.get("active", 1))
    return d

# -> Registrar =============================================
@router.post(
    "",
    response_model=DeviceRegisterOut,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new device and issue credentials.",
)
def register_device(body: DeviceIn, db: DB, _: OperatorUser):
    """
    Cria um registro de dispositivos e retorna suas credenciais.
    """
    device_id = str(uuid.uuid4())
    api_key = generate_api_key()
    api_key_hash = hash_api_key(api_key)

    db.register_device(
        device_id=device_id,
        name=body.name,
        description=body.description,
        topics=body.topics,
        api_key_hash=api_key_hash,
    )

    row = db.search_device(device_id)
    data = _row_to_devices(row)
    data["api_key"] = api_key
    return data

# -> Listar ===========================================
@router.get(
    "",
    response_model=list[DeviceOut],
    summary="List registered devices"
)
def list_devices(
    db: DB,
    pag: Pag,
    active_only: bool = Query(default=True, description="Filter only active devices"),
    _: CurrentUser = None
):
    rows = db.list_device(active_only=active_only, limit=pag.limit, offset=pag.offset)
    return [_row_to_devices(r) for r in rows]

# -> Buscar ============================================
@router.get(
    "/{device_id}",
    response_model=DeviceOut,
    summary="Search for a device by device_id."
)
def search_device(device_id: str, db: DB, _: CurrentUser):
    row = db.search_device(device_id)
    if not row:
        raise HTTPException(
            status_code=404,
            detail="Device not found"
        )
    return _row_to_devices(row)

# -> Atualizar ==========================================
@router.patch(
    "/{device_id}",
    response_model=DeviceOut,
    summary="Update name, description, topics or status active"
)
def update_device(device_id: str, body: DeviceUpdate, db: DB, _: OperatorUser):
    """
    Atualização parcial - apenas os campos enviados no body são modificados.
    Para desativar um dispositivo sem excluí-lo, envie `{"active": false}`.
    Dispositivos inativos deixam de ser autenticados e não têm satatus atualizado.
    """
    ok = db.update_device(
        device_id=device_id,
        name=body.name,
        description=body.description,
        topics=body.topics,
        active=body.active,
    )
    if not ok:
        raise HTTPException(
            status_code=404,
            detail="Device not found."
        )
    return _row_to_devices(db.search_device(device_id))

# -> Renovar api_key =========================================
@router.post(
    "{device_id}/renew",
    response_model=RenewalKeyOut,
    summary="revokes the current api_key and issues a new one."
)
def renew_api_key(device_id: str, db: DB, _: AdminUser):
    """
    Invalida imediatamente a api_key anterior.
    Qualquer dispositivo usandoa chave antiga passará a receber 401.
    A nova chave é retornada apenas nesta resposta.
    """
    new_key = generate_api_key()
    new_hash = hash_api_key(new_key)
    ok = db.renew_api_key(device_id, new_hash)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail="Device not found."
        )
    return {"device_id": device_id, "api_key": new_key}

# -> Mensagens do Dispositivo ===============================
@router.get(
    "/{device_id}/messages",
    response_model=list[MessageOut],
    summary="Messages received from a specific device"
)
def messages_device(device_id: str, db: DB, pag: Pag, _: CurrentUser):
    """
    Retorna o histórico de mensagens vinculadas a este device_id.
    """
    if not db.search_device(device_id):
        raise HTTPException(
            status_code=404,
            detail="Device not found."
        )
    rows = db.messages_device(device_id, limit=pag.limit, offset=pag.offset)
    return [dict(r) for r in rows]
