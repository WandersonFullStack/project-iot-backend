from __future__ import annotations
import asyncio
import json
from contextlib import asynccontextmanager
from typing import Annotated, Optional

from fastapi import (
    Depends, FastAPI, HTTPException, Query,
    WebSocket, WebSocketDisconnect, status
)
from fastapi.responses import JSONResponse

from app.controllers.database_controller import DatabaseController
from app.controllers.callbacks_mqtt_controller import CallbacksMQTTContrller
from app.models.schemas import (
    MessageOut, PublicationIn, PublicationOut,
    StatusOut, PagesParams
)
from app.config.broker_configs import log, mqtt_broker_configs as config

# == INSTÂNCIAS GLOBAIS ===============================================
db = DatabaseController("mqtt_data.db")
mqtt = CallbacksMQTTContrller(db)

# == LIFESPAN -> startup e shutdown gerenciados pelo FastAPI===========
@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    mqtt.start(loop) # conecta ao broker e inicia loop_start()
    yield # aplicação rodando
    mqtt.stop() # disconnect + loop_stop()

# == APP ==============================================================
app = FastAPI(
    title="MQTT Gateway API",
    version="1.0.0",
    description="Publica e recebe mensagens MQTT via REST e WebSocket.",
    lifespan=lifespan
)

# == DEPENDÊNCIAS -> injetadas via Depends()
def get_db() -> DatabaseController:
    """
    Injeta o DatabaseController nas rotas.
    """
    return db

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
DB = Annotated[DatabaseController, Depends(get_db)]
MQTT = Annotated[CallbacksMQTTContrller, Depends(get_mqtt)]
Pag = Annotated[PagesParams, Depends(get_pages)]

# == ROTAS -> /status
@app.get(
    "/api/v1/status",
    response_model=StatusOut,
    summary="MQTT connection status and database statistics",
    tags=["System"]
)
def get_status(db: DB, mqtt: MQTT):
    """
    Retorna o estado atual do cliente MQTT e contagens do banco.
    Útil para health checks e minitoramento.
    """
    return StatusOut(
        mqtt_connected=mqtt.connected,
        broker=config["HOST"],
        client_id=config["CLIENT_ID"],
        total_messages=db.tell_messages(),
        total_publications=db.tell_publications()
    )

# == ROTAS -> /messages
@app.get(
    "/api/v1/messages",
    response_model=list[MessageOut],
    summary="lista mensagens recebidas com paginação e filtro de tópico",
    tags=["Messages"]
)
def list_messages(
    db: DB,
    pag: Pag,
    topic: Optional[str] = Query(
        default=None,
        description="Filtro por tópico. Aceita '%' como wildcard: 'home/%'",
        examples={"exactly": {"value": "home/sensors/temperature"},
                  "wildcard": {"value": "home/%"}}
    )
):
    """
    Retorna as mensagens MQTT armazenadas no banco.
    """
    rows = db.list_messages(topic=topic, limit=pag.limit, offset=pag.offset)
    return [dict(r) for r in rows]

@app.get(
    "/api/v1/messages/{message_id}",
    response_model=MessageOut,
    summary="Search for a message by ID.",
    tags=["Messages"]
)
def search_message(message_id: int, db: DB):
    row = db.search_message(message_id)
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Message id={message_id} not found."
        )
    return dict(row)

@app.delete(
    "/api/v1/messages/{message_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove a message from the database.",
    tags=["Messages"]
)
def delete_message(message_id: int, db: DB):
    """
    Deleta a mensagem localmente - não afeta o broker.
    Retorna 204 No Content em caso de sucesso.
    """
    if not db.delete_message(message_id):
        raise HTTPException(status_code=404, detail="Message not found.")
    
# == ROTAS -> /plublish ================================================
@app.post(
    "/api/v1/publish",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Publish a message MQTT",
    tags=["Publication"]
)
def publish_message(body: PublicationIn, db: DB, mqtt: MQTT):
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
    
    rc, mid = mqtt._publish(
        topic=body.topic,
        payload=body.payload,
        qos=body.qos,
        retain=body.retain,
        content_type=body.content_type,
        expiry_interval=body.expiry_interval,
        user_properties=body.user_properties
    )

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"status": "lined up", "mid": mid, "rc": rc, "topic": body.topic}
    )

# == ROTAS -> /publications =============================================
@app.get(
    "/api/v1/publications",
    response_model=list[PublicationOut],
    summary="List of messages posted by this client.",
    tags=["Publications"]
)
def list_publications(db: DB, pag: Pag):
    """
    Exibe o histórico de publicações deste gateway, incluindo
    o campo `confirm_in` (null = aguardando PUBACK/PUBCOMP do broker).
    """
    rows = db.list_publications(limit=pag.limit, offset=pag.offset)
    return [dict(r) for r in rows]

# == ROTAS -> /topics
@app.get(
    "/api/v1/topics",
    response_model=list[str],
    summary="List the distinct topics already received.",
    tags=["Messages"]
)
def list_topics(db: DB):
    """
    Discovery: retorna todos os tópicos únicos que já chegaram ao gateway.
    """
    return db.distinct_topics()

# == WEBSOCKET -> /ws ==================================================
@app.websocket("/api/v1/ws")
async def websocket_stream(websocket: WebSocket, mqtt: MQTT):
    """
    Stream em tempo real de mensagens MQTT para clientes WebSocket.
    """
    await websocket.accept()

    # Fila com limite de mensagens em buffer por cliente
    q: asyncio.Queue[dict] = asyncio.Queue(maxsize=50)
    mqtt.register_ws_queue(q)

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
        pass
    finally:
        # Garantia de limpeza mesmo em caso de exceção
        mqtt.remove_ws_queue(q)
