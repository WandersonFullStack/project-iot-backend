from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

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
    topic: str = Field(..., exemples=["home/sensors/temperature"])
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
    