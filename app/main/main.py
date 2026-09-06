from __future__ import annotations
import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import Annotated, Optional, AsyncGenerator
from datetime import datetime, timedelta

from fastapi import (
    Depends, FastAPI, HTTPException, Query,
    WebSocket, WebSocketDisconnect, status
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from sqlalchemy.ext.asyncio import AsyncSession

from app.controllers import (
    message_controller as mc,
    user_controller as uc,
    device_controller as dc,
    plc_controller as pc
)
from app.controllers.callbacks_mqtt_controller import CallbacksMQTTContrller

from app.models.schemas import (
    MessageOut, PublicationIn, PublicationOut,
    StatusOut, PagesParams
)
from app.models.schema_orm import MapRegister as MapRegisterORM

from app.config.database import AsyncSessionLocal, engine
from app.config.broker_configs import log, mqtt_broker_configs as config

from app.services.tcp_gateway import TCPGateway
from app.services.modbus_gateway import ModbusGateway, MapRegister
from app.services.protocol_bridge import ProtocolBridge
from app.services.gateway_runtime import (
    ENABLE_UNSCOPED_MODBUS_GATEWAY,
    bridge, modbus_gw, mqtt,
    reload_map_modbus, tcp_gw,
)

from app.routes.router_devices import router as devices_router
from app.routes.router_plcs import router as plcs_router
from app.routes.router_auth import router as auth_router
from app.routes.router_users import router as users_router

from app.auth.user_auth import hash_password, decode_token
from app.auth.dependencies.depends import CurrentUser

# == MANAGED DEPENDENCIES =============================================

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Injeta uma nova sessão assincrona para cada requisição."""

    async with AsyncSessionLocal() as session:
        yield session

DB = Annotated[AsyncSession, Depends(get_db)]

# == TASKS OF BACKGROUND ==============================================

async def _monitor_offline_devices(timeout_min: int = 5) -> None:
    """
    Tarefa asyncio que roda em background e marca como offline qualquer
    dispositivo que não publicou mensagem nos últimos minutos.
    """
    async with AsyncSessionLocal() as db:
        await dc.update_offline_devices(db, timeout=timeout_min)
        await db.commit()

        log.info(f"Offline devices monitoring: checked at {datetime.now().isoformat()}")



# == LIFESPAN -> startup e shutdown gerenciados pelo FastAPI===========
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia recursos globais: startup e shutdown do FastAPI."""

    # -> STARTUP

    log.info("Starting MQTT Gateway...")

    # Inicia todos os servidores concorrentemente
    loop = asyncio.get_event_loop()
    mqtt.start(loop)

    await tcp_gw.start()
    if ENABLE_UNSCOPED_MODBUS_GATEWAY:
        log.warning(
            "Starting the shared Modbus gateway. This endpoint is not "
            "tenant-aware and must only be exposed in a trusted network."
        )
        await modbus_gw.start()
        await reload_map_modbus()
    
    log.info(
        "MQTT and TCP gateways started; shared Modbus gateway enabled=%s",
        ENABLE_UNSCOPED_MODBUS_GATEWAY,
    )

    # iniciar monitoramento de dispositivos offline em background
    async def _loop_monitor():
        while True:
            try:
                await asyncio.sleep(60)
                await _monitor_offline_devices(timeout_min=5)
            except Exception as e:
                log.error(f"Error in offline devices monitor: {e}")

    task = asyncio.create_task(_loop_monitor())

    yield # aplicação rodando

    # -> SHUTDOWN
    log.info("Shutting down MQTT Gateway...")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    await tcp_gw.stop()
    if ENABLE_UNSCOPED_MODBUS_GATEWAY:
        await modbus_gw.stop()
    mqtt.stop() # disconnect + loop_stop()
    await engine.dispose()

    log.info("MQTT Gateway stopped")

# == APP ==============================================================
app = FastAPI(
    title="MQTT Gateway API",
    version="1.0.0",
    description="Publica e recebe mensagens MQTT via REST e WebSocket.",
    lifespan=lifespan
)

origins = [
    "http://localhost:5173",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(devices_router)
app.include_router(plcs_router)
app.include_router(auth_router)
app.include_router(users_router)

# == GLOBAL DEPENDENCIES ============================================
def get_mqtt() -> CallbacksMQTTContrller:
    """
    Injeta o CallbacksMQTTController nas rotas que presisam publicar mensagens.
    Utilizado para checar o status de conexão.
    """
    return mqtt

def get_pages(
        limit: int = Query(default=20, ge=1, le=100, description="Items per page"),
        offset: int = Query(default=0, ge=0, description="Initial displacement")
) -> PagesParams:
    """
    Parâmetros de paginação reutilizáveis.
    """
    return PagesParams(limit=limit, offset=offset)

# Aliases de tipo para injeção mais limpas nos handlers

MQTT = Annotated[CallbacksMQTTContrller, Depends(get_mqtt)]
Pag = Annotated[PagesParams, Depends(get_pages)]

# == ROTAS -> /status
@app.get("/", tags=["System"])
async def root():
    return {"message": "MQTT Gateway API"}

@app.get(
    "/api/v1/status",
    response_model=StatusOut,
    summary="MQTT connection status and database statistics",
    tags=["System"]
)
async def get_status(db: DB, user: CurrentUser):
    """
    Retorna o estado atual do cliente MQTT e contagens do banco.
    Útil para health checks e monitoramento.
    """
    msg_count = await mc.count_messages(db, user.id)
    pub_count = await mc.count_publications(db, user.id)

    return StatusOut(
        mqtt_connected=mqtt.connected,
        broker=config["HOST"],
        client_id=config["CLIENT_ID"],
        total_messages=msg_count,
        total_publications=pub_count
    )

# == ROTAS -> /messages
@app.get(
    "/api/v1/messages",
    response_model=list[MessageOut],
    summary="lista mensagens recebidas com paginação e filtro de tópico",
    tags=["Messages"]
)
async def list_messages(
    db: DB,
    pag: Pag,
    user: CurrentUser,
    topic: Optional[str] = Query(
        default=None,
        description="Topic filter. Accepts '%' wildcard: 'home/%'",
    ),
):
    """
    Retorna as mensagens MQTT armazenadas no banco com paginação.
    """
    return await mc.list_message(
        db,
        user.id,
        topic=topic,
        limit=pag.limit,
        offset=pag.offset,
    )

@app.get(
    "/api/v1/messages/{message_id}",
    response_model=MessageOut,
    summary="Search for a message by ID.",
    tags=["Messages"]
)
async def search_message(message_id: int, db: DB, user: CurrentUser):
    message = await mc.search_message(db, message_id, user.id)
    if not message:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Message not found."
        )
    return message

@app.delete(
    "/api/v1/messages/{message_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove a message from the database.",
    tags=["Messages"]
)
async def delete_message(message_id: int, db: DB, user: CurrentUser):
    """
    Deleta a mensagem localmente - não afeta o broker.
    Retorna 204 No Content em caso de sucesso.
    """
    ok = await mc.delete_message(db, message_id, user.id)
    if not ok:
        raise HTTPException(status_code=404, detail="Message not found.")
    
    await db.commit()
    
# == ROTAS -> /plublish ================================================
@app.post(
    "/api/v1/publish",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Publish a message MQTT",
    tags=["Publication"]
)
async def publish_message(body: PublicationIn, db: DB, user: CurrentUser):
    """
    Enfileira uma publicação no broker MQTT com propriedes.

    Retorna 202 Accepted porque a entrega ao broker é assíncrona -> a confirmação
    real chega no callback `on_publish`, que atualiza o campo `confirm_in`
    no banco de dados.
    """
    if not mqtt.connected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="The MQTT client is not connected to the broker."
        )

    device = await dc.search_device_by_topic(db, body.topic, user.id)
    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No owned active device is authorized for this topic.",
        )
    
    rc, mid = await mqtt.publish(
        device_id=device.device_id,
        topic=body.topic,
        payload=body.payload,
        qos=body.qos,
        retain=body.retain,
        content_type=body.content_type,
        expiry_interval=body.expiry_interval,
        user_properties=[
            ("published_by", str(user.id)),
            ("device_id", device.device_id),
            *(body.user_properties or []),
        ],
    )

    return {
        "status": "quered",
        "mid": mid,
        "rc": rc,
        "topic": body.topic
    }

# == ROTAS -> /publications =============================================
@app.get(
    "/api/v1/publications",
    response_model=list[PublicationOut],
    summary="List of messages posted by this client.",
    tags=["Publications"]
)
async def list_publications(db: DB, pag: Pag, user: CurrentUser):
    """
    Exibe o histórico de publicações deste gateway, incluindo
    o campo `confirm_in` (null = aguardando PUBACK/PUBCOMP do broker).
    """
    return await mc.list_publication(
        db, user.id, limit=pag.limit, offset=pag.offset
    )

# == ROTAS -> /topics
@app.get(
    "/api/v1/topics",
    response_model=list[str],
    summary="List the distinct topics already received.",
    tags=["Messages"]
)
async def list_topics(db: DB, user: CurrentUser):
    """
    Discovery: retorna todos os tópicos únicos que já chegaram ao gateway.
    """
    return await mc.distinct_topics(db, user.id)

# == WEBSOCKET -> /ws ==================================================
@app.websocket("/api/v1/ws")
async def websocket_stream(
        websocket: WebSocket, 
        token: str = Query(..., description="JWT of access")
):
    """
    Stream em tempo real de mensagens MQTT para clientes WebSocket.
    """
    from jose import JWTError

    try:
        payload = decode_token(token)
        user_id = int(payload.get("sub", 0))

    except (JWTError, ValueError):
        await websocket.close(code=4001, reason="Invalid token")
        return
        
    async with AsyncSessionLocal() as db:
        user = await uc.search_user_by_id(db, user_id)

        if not user or not user.active:
            await websocket.close(
                code=4001,
                reason="User not found or inactive"
            )
            return
        device_ids = await dc.list_device_ids(db, user_id)

    await websocket.accept()
    
    # Fila com limite de mensagens em buffer por cliente
    q: asyncio.Queue[dict] = asyncio.Queue(maxsize=50)
    mqtt.register_ws_queue(q, device_ids)

    try:
        while True:
            """
            await suspende a coroutine até que um item esteja disponível.
            Nenhuma CPU é consumida enquanto aguarda -> o event loop serve 
            outras requisições normalmente.
            """
            data = await q.get()
            await websocket.send_text(json.dumps(data, default=str))
    except WebSocketDisconnect:
        log.info(f"WebSocket client disconnected: user_id={user_id}")
    except Exception as e:
        log.info(f"WebSocket error: {e}")
    finally:
        # Garantia de limpeza mesmo em caso de exceção
        mqtt.remove_ws_queue(q)
