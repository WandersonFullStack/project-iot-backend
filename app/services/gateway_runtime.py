from __future__ import annotations
import os

from app.config.broker_configs import log
from app.config.database import AsyncSessionLocal
from app.controllers import plc_controller as pc
from app.controllers.callbacks_mqtt_controller import CallbacksMQTTContrller
from app.services.modbus_gateway import ModbusGateway, MapRegister
from app.services.protocol_bridge import ProtocolBridge
from app.services.tcp_gateway import TCPGateway

"""
Instâncias unicas dos gateways. Vivem aqui, e não em main.py, para que as
rotas possam deisparar reload_map_modbus sem importar a aplicação.
"""

mqtt = CallbacksMQTTContrller()
bridge = ProtocolBridge(mqtt)
tcp_gw = TCPGateway(
    bridge,
    host=os.getenv("TCP_GATEWAY_HOST", "0.0.0.0"),
    port=int(os.getenv("TCP_GATEWAY_PORT", "9000")),
)
modbus_gw = ModbusGateway(
    bridge,
    host=os.getenv("MODBUS_HOST", "0.0.0.0"),
    port=int(os.getenv("MODBUS_PORT", "502")),
)
ENABLE_UNSCOPED_MODBUS_GATEWAY = (
    os.getenv("ENABLE_UNSCOPED_MODBUS_GATEWAY", "false").lower() == "true"
)

async def reload_map_modbus() -> None:
    """
    Carrega o mapa atual do banco e reconstroi o datastore do ModbusGateway.
    Chama em background apos criar, atualizar ou deletar PLCs e 
    registradores -> garante que endereços removidos deixem de publicar.
    """
    if not ENABLE_UNSCOPED_MODBUS_GATEWAY:
        return

    async with AsyncSessionLocal() as db:
        registers = await pc.load_map_modbus(db)
        modbus_gw.map = [
            MapRegister(
                address=r["address"],
                topic=r["topic"],
                unit=r["unit"] or "",
                scale=r["scale"],
                device_id=r["device_id"],
            )
            for r in registers
        ]
        log.info("Map Modbus reloaded: %d registers active.", len(modbus_gw.map))