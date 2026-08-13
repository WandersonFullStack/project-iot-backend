from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime
from enum import Enum

class TypeRegister(str, Enum):
    holding = "holding"   # FC 03/06/16 -> leitura/escrita, 16 bits
    coil = "coil"   # FC 01/05/15 -> leitura/escrita, 1 bit (saidas digitais)
    input = "input"   #FC 04 -> somente leitura, 16 bits (entradas analógicas)
    discrete = "discrete"   # FC 02 -> somente leitura, 1 bit (entradas digitais)

class PLCIn(BaseModel):
    device_id: str = Field(..., description="UUID of the associated device")
    name: str = Field(..., min_length=2, max_length=80)
    ip: str = Field(..., examples=["192.168.1.10"])
    port_modbus: int = Field(default=502, ge=1, le=65535)
    port_tcp: int = Field(default=9000, ge=1, le=65535)
    protocol: Literal["modbus", "tcp", "ambos"] = "modbus"
    description: Optional[str] = None
    unit_id: int = Field(default=255, ge=0, le=255, description="Modbus address of the slave (255=broadcast)")
    timeout: float = Field(default=5.0, ge=0.5, le=60.0)

class PLCOut(BaseModel):
    id: int
    device_id: str
    name: str
    description: Optional[str]
    ip: str
    port_modbus: int
    port_tcp: int
    protocol: str
    unit_id: int
    timeout: float
    active: bool
    created_in: datetime
    total_registers: int = 0

    model_config = {"from_attributes": True}

class PLCUpdate(BaseModel):
    name: Optional[str] = None
    ip: Optional[str] = None
    port_modbus: Optional[int] = Field(default=None, ge=1, le=65535)
    port_tcp: Optional[int] = Field(default=None, ge=1, le=65535)
    protocol: Optional[Literal["modbus", "tcp", "ambos"]] = None
    description: Optional[str] = None
    unit_id: Optional[int] = Field(default=None, ge=0, le=255)
    timeout: Optional[float] = Field(default=None, ge=0.5, le=60.0)
    active: Optional[bool] = None

class MapRegisterIn(BaseModel):
    type: TypeRegister
    tag_name: str = Field(..., min_length=1)
    address: int = Field(..., ge=0, le=65534)
    topic: str = Field(..., examples=["boiler/temperature"])
    description: Optional[str] = None
    unit: Optional[str] = Field(default=None, examples=["°C", "bar", "L/h"])
    scale: float = Field(default=1.0, description="real_value = gross x scale + offset")
    offset: float = 0.0
    qos: int = Field(default=1, ge=0, le=2)
    read_only: bool = True

class MapRegisterOut(BaseModel):
    id: int
    plc_id: int
    type: str
    tag_name: str
    address: int
    address_modbus: int # calculado: endereço + offset do type
    topic: str
    description: Optional[str]
    unit: Optional[str]
    scale: float
    offset: float
    qos: int
    read_only: bool
    active: bool
    created_in: datetime

    model_config = {"from_attributes": True}

class MapRegisterUpdate(BaseModel):
    tag_name: Optional[str] = None
    topic: Optional[str] = None
    description: Optional[str] = None
    unit: Optional[str] = None
    scale: Optional[float] = None
    offset: Optional[float] = None
    qos: Optional[int] = Field(default=None, ge=0, le=2)
    read_only: Optional[bool] = None
    active: Optional[bool] = None

class MapBulkIn(BaseModel):
    """Importação em lote -> usa INSERT OR REPLACE, portanto é idempotente."""
    registers: list[MapRegisterIn] = Field(..., min_length=1, max_length=500)

class TestConnectionOut(BaseModel):
    success: bool
    message: str
    ip: str
    port: int
    unit_id: int
    time_ms: Optional[float] = None
    value_reg0: Optional[int] = None    # valor bruto do registrador 0 (holding)

class DeviceIn(BaseModel):
    """Payload para registrar um novo dispositivo."""
    name: str = Field(..., min_length=2, max_length=80, examples=["Room sensor"])
    description: Optional[str] = Field(default=None, max_length=255)
    topics: list[str] = Field(
        ...,
        min_length=1,
        description="List of topics that the device can publish on.",
        examples=[["house/sensors/temperature", "house/sensors/humidity"]]
    )

class DeviceUpdate(BaseModel):
    """Campos atualizaveis após o registro."""
    name: Optional[str] = Field(default=None, min_length=2, max_length=80)
    description: Optional[str] = None
    topics: Optional[list[str]] = None
    active: Optional[bool] = None

class DeviceOut(BaseModel):
    """Representação pública de um dispositivo (sem api_key)."""
    device_id: str
    name: str
    description: Optional[str]
    topics: list[str]
    status: str
    last_contact: Optional[datetime]
    created_in: datetime
    active: bool

    model_config = {"from_attributes": True}

class DeviceRegisterOut(DeviceOut):
    """
    Retorna apenas no momento do registro.
    Inclui a api_key em texto puro -> ela não é armazenada,
    apenas seu hash. Após esta resposta, a chave não pode ser recuperada.
    """
    api_key: str = Field(
        ...,
        description="Keep this key safe. It will not be shown again."
    )

class RenewalKeyOut(BaseModel):
    """Retornado ao revogar e gerar uma nova api_key."""
    device_id: str
    api_key: str = Field(..., description="New api_key. The previous one has been invalidated.")

class MessageOut(BaseModel):
    id: int
    topic: str
    payload: Optional[str]
    qos: int
    retain: bool
    content_type: Optional[str]
    user_props: Optional[str]
    received_in: datetime

    model_config = {"from_attributes": True}

class PublicationIn(BaseModel):
    topic: str = Field(..., examples=["house/sensors/temperature"])
    payload: str = Field(..., examples=['{"value": 23.5}'])
    qos: int = Field(default=1, ge=0, le=2)
    retain: bool = False
    content_type: str = "application/json"
    expiry_interval: Optional[int] = Field(default=None, ge=1, description="TTL em segundos")
    user_properties: Optional[list[tuple[str, str]]] = Field(
        default=None, examples=[[("sensor_id", "t-01")]]
    )

class PublicationOut(BaseModel):
    id: int
    topic: str
    payload: Optional[str]
    qos: int
    mid: int
    confirmed_in: Optional[datetime]

    model_config = {"from_attributes": True}

class StatusOut(BaseModel):
    mqtt_connected: bool
    broker: str
    client_id: str
    total_messages: int
    total_publications: int

class PagesParams(BaseModel):
    """Parâmetros de paginação reutilizáveis via Depends()."""

    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)
    