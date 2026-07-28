from __future__ import annotations
import asyncio
import json
from dataclasses import dataclass
from typing import Callable

from pymodbus.datastore import (
    ModbusServerContext,
    ModbusDeviceContext,
    ModbusSparseDataBlock,
)
from pymodbus.server import StartAsyncTcpServer
from pymodbus.pdu.device import ModbusDeviceIdentification

from app.config.database import AsyncSessionLocal
from app.services.protocol_bridge import ProtocolBridge
from app.config.broker_configs import log
from app.controllers import plc_controller as pc


# == MAPEAMENTO DE REGISTRADORES -> TÓPICOS MQTT ==================
@dataclass
class MapRegister:
    """
    Define como um registrador Modbus se traduz para um tópico MQTT.
    """
    address: int
    topic: str
    unit: str
    scale: float
    device_id: str

# == DATABLOCK COM CALLBACK DE ESCRITA ============================
class CallbackDataBlock(ModbusSparseDataBlock):
    """
    Estende ModbusSparseDataBlock para interceptar escritas do CLP.
    """
    def __init__(self, map: list[MapRegister], callback: Callable):
        # Inicializa o store com zeros nos endereços mapeados
        initial_values = {m.address: 0 for m in map} or {0: 0}
        super().__init__(initial_values)
        self._map_by_address = {m.address: m for m in map}
        self._callback = callback

    def setValues(self, address: int, values: list[list]):
        super().setValues(address, values) # salva no store interno primeiro

        for i, gross_value in enumerate(values):
            address = address + i
            map = self._map_by_address.get(address)
            if not map:
                continue # registrador não mapeado -> ignora

            real_value = round(gross_value * map.scale, 4)
            self._callback(map, real_value, gross_value)

# == SERVIDOR MODBUS TCP ========================================
class ModbusGateway:
    """
    Servidor Modbus TCP que expõe registradores mapeados para CLPs.
    """
    def __init__(
            self,
            bridge: ProtocolBridge,
            map: list[MapRegister] | None = None,
            host: str = "0.0.0.0",
            port: int = 502,
    ):
        self.bridge = bridge
        self.map = map
        self.host = host
        self.port = port
        self._task: asyncio.Task | None = None

    async def _on_writing(
            self,
            map: MapRegister,
            real_value: float,
            gross_value: int
    ):
        """
        Callback síncrono chamado pelo pymodbus na thread do servidor Modbus.
        """
        log.info(
            "Modbus writing | reg=%d value=%s %s -> %s",
            map.address, real_value, map.unit, map.topic,
        )
        async with AsyncSessionLocal() as db:
            try:
                await self.bridge.to_forward(
                    db,
                    device_id=map.device_id,
                    topic=map.topic,
                    payload={
                        "value": real_value,
                        "gross_value": gross_value,
                        "unit": map.unit,
                        "registrar": map.address,
                    },
                )
            except Exception as e:
                log.error("Error forwarding Modbus reading: %s", e)

    def _build_context(self) -> ModbusServerContext:
        """
        Monta o contexto Modbus com o datablock customizado.

        ModbusDeviceContext agrupa os quatro tipos de dados Modbus:
            di = Discrete Inputs (somente leitura, 1 bit)
            co = Coils (leitura/escrita, 1 bit)
            hr = Holding Registers (leitura/escrita, 16 bits)
            ir = Input Registers (somente leitura, 16 bits)
        """
        datablock = CallbackDataBlock(self.map or [], self._on_writing)
        empty = ModbusSparseDataBlock({0: 0})
        slave = ModbusDeviceContext(
            hr=datablock if self.map else empty,   # holding registers com callback
            di=empty,
            co=empty,
            ir=empty
        )
        return ModbusServerContext(devices={0xFF: slave})
    
    def _build_identity(self) -> ModbusDeviceIdentification:
        """
        Metadados opcionais retornados ao CLP na FC 43 (Read Device Identification).
        Facilita o diagnóstico na feramenta de engenharia do CLP.
        """
        ident = ModbusDeviceIdentification()
        ident.VendorName = "MQTT Industrial Gateway"
        ident.ProductCode = "MQTT-GW-v1"
        ident.VendorUrl = "https://magautomations.com.br"
        ident.ProductName = "Gateway MQTT"
        ident.ModelName = "FastAPI Edition"
        ident.MajorMinorRevision = "1.0"
        
        return ident
    
    async def read_and_forward(
            self,
            plc_id: int,
            register_id: int,
            value: int
    ):
        """
        Lê um registrador Modbus e publica no MQTT via bridge.
        """
        async with AsyncSessionLocal() as db:
            # Buscar PLC e registrador
            register = await pc.search_register(db, plc_id, register_id)
            plc = await pc.search_plc(db, plc_id)

            if not register or not plc:
                return
            
            # Aplicar scale/offset
            real_value = value * register.scale + register.offset

            metric = register.topic
            topic = f"application/devices/{plc.device_id}/{metric}"

            # Publicar via bridge
            msg_id = await self.bridge.to_forward(
                db,
                device_id=plc.device_id,
                topic=topic,
                payload={"value": real_value, "unit": register.unit},
                qos=register.qos
            )
            await db.commit()

            return msg_id
    
    async def start(self):
        """
        StartAsyncTcpServer é uma coroutine que roda indefinidamente.
        Rodamos como asyncio.Task para não bloquear o event loop do FastAPI.
        """
        context = self._build_context()
        identity = self._build_identity()

        async def _run():
            log.info("Modbus TCP waiting in %s:%d", self.host, self.port)
            await StartAsyncTcpServer(
                context=context,
                identity=identity,
                address=(self.host, self.port)
            )

        self._task = asyncio.create_task(_run())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            log.info("Modbus Gateway closed.")
            