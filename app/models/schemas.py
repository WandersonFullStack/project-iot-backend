from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

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
    