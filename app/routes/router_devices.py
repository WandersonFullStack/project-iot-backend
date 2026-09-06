from __future__ import annotations
import uuid
import json
from typing import Annotated, AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException, Query, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import AsyncSessionLocal
from app.controllers import device_controller as dc, plc_controller as pc
from app.services.gateway_runtime import reload_map_modbus

from app.models.schemas import (
    DeviceIn, DeviceUpdate, DeviceOut,
    DeviceRegisterOut, RenewalKeyOut,
    MessageOut, PagesParams, PLCOut
)
from app.auth.device_auth import generate_api_key, hash_api_key
from app.auth.dependencies.depends import CurrentUser

router = APIRouter(prefix="/api/v1/devices", tags=["Devices"])

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

DB = Annotated[AsyncSession, Depends(get_db)]

def _page_params(
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0)
) -> PagesParams:
    return PagesParams(limit=limit, offset=offset)

Pag = Annotated[PagesParams, Depends(_page_params)]


# -> Registrar =============================================
@router.post(
    "",
    response_model=DeviceRegisterOut,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new device and issue credentials.",
)
async def register_device(body: DeviceIn, db: DB, user: CurrentUser):
    """
    Cria um registro de dispositivos e retorna suas credenciais.
    A api_key é exibida apenas nesta resposta — não pode ser recuperada depois.
    """
    device_id = str(uuid.uuid4())
    api_key = generate_api_key()
    api_key_hash = hash_api_key(api_key)

    device = await dc.register_device(
        db,
        user_id=user.id,
        device_id=device_id,
        name=body.name,
        description=body.description,
        topics=body.topics,
        api_key_hash=api_key_hash,
    )

    await db.commit()
    await db.refresh(device)
    
    return {**DeviceOut.model_validate(device).model_dump(), "api_key": api_key}

# -> Listar ===========================================
@router.get(
    "",
    response_model=list[DeviceOut],
    summary="List registered devices"
)
async def list_devices(
    db: DB,
    pag: Pag,
    user: CurrentUser,
    active_only: bool = Query(default=True, description="Filter only active devices"),
):
    return await dc.list_device(
        db,
        user_id=user.id,
        active_only=active_only,
        limit=pag.limit,
        offset=pag.offset,
    )


# -> Buscar ============================================
@router.get(
    "/{device_id}",
    response_model=DeviceOut,
    summary="Search for a device by device_id."
)
async def search_device(device_id: str, db: DB, user: CurrentUser):
    device = await dc.search_device(db, device_id, user.id)
    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found"
        )
    
    return device

@router.get(
        "/{device_id}/plc",
        response_model=PLCOut | None,
        summary="Search PLC linked to a device"
)
async def search_plc_by_device(device_id: str, db: DB, user: CurrentUser):
    device = await dc.search_device(db, device_id, user.id)

    if not device:
        raise HTTPException(
            status_code=404,
            detail="Device not found"
        )
    
    plc = await pc.search_plc_by_device_id(db, device_id, user.id)

    return plc

# -> Atualizar ==========================================
@router.patch(
    "/{device_id}",
    response_model=DeviceOut,
    summary="Update name, description, topics or status active"
)
async def update_device(device_id: str, body: DeviceUpdate, db: DB, user: CurrentUser):
    """
    Atualização parcial - apenas os campos enviados no body são modificados.
    Para desativar um dispositivo sem excluí-lo, envie `{"active": false}`.
    """
    ok = await dc.update_device(
        db,
        user_id=user.id,
        device_id=device_id,
        **{k: v for k, v in body.model_dump().items() if v is not None},
    )
    if not ok:
        raise HTTPException(
            status_code=404,
            detail="Device not found."
        )
    await db.commit()

    device = await dc.search_device(db, device_id, user.id)
    return device

# -> Delete ==================================================
@router.delete(
    "/{device_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Permanently delete a device and everything under it"
)
async def delete_device(
    device_id: str,
    db: DB,
    bg: BackgroundTasks,
    user: CurrentUser,
):
    """
    Remove em cascata o PLC vinculado, os registradores desse PLC e todo o
    histórico de mensagens e publicações do dispositivo. Irreversivel.

    Para apenas suspender o dispositivo, use PATCH com `{"active": false}`.
    """
    ok = await dc.delete_device(db, device_id, user.id)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail="Device not found."
        )
    
    await db.commit()
    bg.add_task(reload_map_modbus)

# -> Renovar api_key =========================================
@router.post(
    "/{device_id}/renew",
    response_model=RenewalKeyOut,
    summary="revokes the current api_key and issues a new one."
)
async def renew_api_key(device_id: str, db: DB, user: CurrentUser):
    """
    Invalida imediatamente a api_key anterior.
    Qualquer dispositivo usandoa chave antiga passará a receber 401.
    A nova chave é retornada apenas nesta resposta.
    """
    new_key = generate_api_key()
    new_hash = hash_api_key(new_key)
    ok = await dc.renew_api_key(db, device_id, user.id, new_hash)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail="Device not found."
        )
    
    await db.commit()
    return {"device_id": device_id, "api_key": new_key}

# -> Mensagens do Dispositivo ===============================
@router.get(
    "/{device_id}/messages",
    response_model=list[MessageOut],
    summary="Messages received from a specific device"
)
async def messages_device(device_id: str, db: DB, pag: Pag, user: CurrentUser):
    """
    Retorna o histórico de mensagens vinculadas a este device_id.
    """
    if not await dc.search_device(db, device_id, user.id):
        raise HTTPException(
            status_code=404,
            detail="Device not found."
        )
    result = await dc.messages_device(
        db, 
        device_id,
        user.id,
        limit=pag.limit, 
        offset=pag.offset
    )
    
    return result
