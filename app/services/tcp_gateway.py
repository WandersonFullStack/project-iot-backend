from __future__ import annotations
import asyncio
import json
import time

from app.config.database import AsyncSessionLocal

from .protocol_bridge import ProtocolBridge
from app.config.broker_configs import log
from app.config.tcp_configs import tcp_limits_config as config

class TCPSession:
    """Representa uma conexão TCP ativa com um único CLP/dispositivo."""

    def __init__(
            self,
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
            bridge: ProtocolBridge
    ):
        self.reader = reader
        self.writer = writer
        self.bridge = bridge
        self.device: dict | None = None
        self.last_frame = time.monotonic()
        self._peer = writer.get_extra_info("peername") # (ip, port)

    async def _send(self, frame: dict):
        """
        Serializa o frame como JSON + \\n e envia ao cliente.
        drain() garante que o buffer do SO foi esvaziado antes de continuar
        -> envia acúmulo infinito de dados em caso de cliente lento.
        """
        data = json.dumps(frame, default=str).encode() + b"\n"
        self.writer.write(data)
        await self.writer.drain()

    async def _read_frame( self) -> dict | None:
        """Lê uma linha do stream até \\n e desserializa como JSON."""

        try:
            raw = await asyncio.wait_for(
                self.reader.readuntil(b"\n"),
                timeout=config["IDLE_TIMEOUT"]
            )
        except asyncio.TimeoutError:
            log.warning("[%s] Session ended due to idle timeout", self._peer)
            return None
        except asyncio.LimitOverrunError:
            log.warning(
                "[%s] Frame exceeded %d bytes -> connection closed",
                self._peer, 
                config["MAX_FRAME_BYTES"]
            )
            return None
        except(asyncio.IncompleteReadError, ConnectionAbortedError):
            return None # cliente desconectou
        
        if len(raw) > config["MAX_FRAME_BYTES"]:
            return None
        
        self.last_frame = time.monotonic()
        try:
            return json.loads(raw.decode("utf-8").strip())
        except json.JSONDecodeError as e:
            log.warning("[%s] JSON invalid: %s", self._peer, e)
            await self._send({"type": "error", "reason": "JSON invalid"})
            return {} # frame inválido, mas mantém sessão

    def _extract_payload(self, frame: dict, excluded_keys: tuple[str, ...]) -> object:
        """
        Aceita payload escalar em:
        - payload
        - value
        - ou, como fallback, um único campo extra
        """
        if "payload" in frame:
            return frame["payload"]

        if "value" in frame:
            return frame["value"]

        extra = {k: v for k, v in frame.items() if k not in excluded_keys}
        if len(extra) == 1:
            return next(iter(extra.values()))

        return extra
    
    async def _authenticate(self) -> bool:
        """
        Aguarda o frame de autentidação dentro do timeout.
        Qualquer outro frame antes da autenticação é recusado.
        """
        try:
            frame = await asyncio.wait_for(self._read_frame(), timeout=config["AUTH_TIMEOUT"])
        except asyncio.TimeoutError:
            log.warning("[%s] Timeout of authentication", self._peer)
            return False
        
        if not frame or frame.get("type") != "auth":
            await self._send({"type": "auth_fail", "reason": "First frame must be 'auth'"})
            return False
        
        device_id = frame.get("device_id", "")
        api_key = frame.get("api_key", "")
        async with AsyncSessionLocal() as db:
            dev = await self.bridge.authenticate(db, device_id, api_key)
        
        if not dev:
            await self._send({"type": "auth_fail", "reason": "Credetials invalid"})
            return False
        
        self.device = dev
        await self._send({"type": "auth_ok", "device_id": device_id, "name": dev["name"]})
        log.info("[%s] Authenticated: %s (%s)", self._peer, device_id, dev["name"])
        return True
        
    async def _process_data(self, frame: dict):
        """
        Frame simples de dados único.
        O gateway monta o payload JSON e encaminha via ProtocolBridge.
        """
        metric = frame.get("metric")
        if not metric:
            await self._send({"type": "error", "reason": "Field 'metric' absent"})
            return

        device_id = self.device["device"]
        topic = f"application/devices/{device_id}/{metric}"
        
        # Monta o payload preservando todos campos extras do frame
        payload = self._extract_payload(frame, ("type", "metric"))

        async with AsyncSessionLocal() as db:
            row_id = await self.bridge.to_forward(
                db,
                device_id=self.device["device"],
                topic=topic,
                payload=payload,
            )
        
        await self._send({"type": "ack", "id": row_id})

    async def _process_batch(self, frame: dict):
        """
        Frame de múltiplas leituras -> útil para CLPs que agrupam dados
        de vários sensores em uma única transmissão para economizar overhead TCP.
        """
        readings = frame.get("readings", [])
        if len(readings) > config["MAX_BATCH_READING"]:
            await self._send({
                "type": "error", 
                "reason": f"Batch exceeds {config["MAX_BATCH_READING"]} items"
            })
            return
        
        ids = []
        for read in readings:
            metric = read.get("metric")
            if not metric:
                continue

            device_id = self.device["device"]
            topic = f"application/devices/{device_id}/{metric}"
            payload = self._extract_payload(read, ("topic",))
            
            async with AsyncSessionLocal() as db:
                row_id = await self.bridge.to_forward(
                    db,
                    device_id=self.device["device"],
                    topic=topic,
                    payload=payload,
                )
            ids.append(row_id)

        await self._send({"type": "ack", "batch_ids": ids, "count": len(ids)})
        
    async def run(self):
        """
        Loop principal da sessão. Cada interação lê um frame e despacha
        para o handler adequado. Encerra quando o cliente desconecta,
        idle timeout expira ou frame "disconect" é recebido.
        """
        if not await self._authenticate():
            self.writer.close()
            return
        
        while True:
            frame = await self._read_frame()

            if frame is None: # cliente desconectou ou timeout
                break
            if not frame:
                continue

            type = frame.get("type", "")

            if type == "data":
                await self._process_data(frame)

            elif type == "batch":
                await self._process_batch(frame)

            elif type == "ping":
                # Keep-alive: importante para CLPs atrás de firewalls
                # que fecham conexões TCP inativas
                await self._send({"type": "pong"})

            elif type == "disconnect":
                log.info("[%s] Disconnection requested by the customer", self._peer)
                break

            else:
                await self._send({"type": "error", "reason": f"Type '{type}' unknown"})

        log.info("[%s] Session closed", self._peer)
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass

class TCPGateway:
    """
    Servidor TCP asyncio que aceita múltiplas conexões simutâneas de CLPs.

    Cada conexão gera uma TCPSession independente rodando como coroutine,
    o evento loop do asyncio multplexa todas as sessões sem threads extras.
    Isso permite centenas de CLPs conectados simutaneamente com baixo consumo.
    """
    def __init__(
            self,
            bridge: ProtocolBridge,
            host: str = "0.0.0.0",
            port: int = 9000
    ):
        self.bridge = bridge
        self.host = host
        self.port = port
        self._server: asyncio.Server | None = None

    async def handle_connection(
            self,
            device_id: str,
            api_key: str,
            data: dict
    ):
        """
        Quando um cliente TCP conecta, autentica e envia dados.
        """
        async with AsyncSessionLocal() as db:
            device = await self.bridge.authenticate(db, device_id, api_key)

            if not device:
                log.info(f"Auth failed for {device_id}")

                return
            
            msg_id = await self.bridge.to_forward(
                db,
                device_id=device_id,
                topic=f"application/devices/{device_id}/data",
                payload=data,
                qos=1
            )
            log.info(f"TCP message forwarded: msg_id={msg_id}")

    async def _handle_client(
            self,
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter
    ):
        """
        Callback do asyncio.start_server -> chamado para cada nova conexão.
        Cria uma sessão e a executa até o encerramento.
        """
        session = TCPSession(reader, writer, self.bridge)
        await session.run()

    async def start(self):
        """
        asyncio.start_server() cria o socket TCP e registra callback
        no event loop. A coroutine retorna imediatamente -> o servidor
        aceita conexões em background enquanto o event loop roda.
        """
        self._server = await asyncio.start_server(
            self._handle_client,
            host=self.host,
            port=self.port,
            limit=config["MAX_FRAME_BYTES"]
        )
        log.info("TCP Gateway waiting in %s:%d", self.host, self.port)

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            log.info("TCP Gateway closed")
        