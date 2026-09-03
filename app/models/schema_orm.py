from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Integer, String, Boolean, UniqueConstraint,
    Float, ForeignKey, Text, DateTime, Index
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.config.database import Base
from app.config.modbus_configs import _OFFSET_MODBUS

class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        Index("index_user_username", "username"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(40), unique=True, nullable=False)
    email: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(80))
    password_hash: Mapped[str] = mapped_column(String, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_in: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=lambda: datetime.now(timezone.utc)
    )

    tokens: Mapped[list["RefreshToken"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan"
    )
    devices: Mapped[list["Device"]] = relationship(back_populates="owner")

class RefreshToken(Base):
    __tablename__ = "refresh_tokens"
    __table_args__ = (
        Index("index_reftokens_token_hash", "token_hash"),
        Index("index_reftokens_user_id", "user_id")
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"))
    token_hash: Mapped[str] = mapped_column(String, unique=True)
    created_in: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc)
    )
    expires_in: Mapped[datetime] =mapped_column(DateTime(timezone=True))
    revoked: Mapped[bool] = mapped_column(Boolean, default=False)

    user: Mapped[User] = relationship(back_populates='tokens')

class Device(Base):
    __tablename__ = "devices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # Legacy rows remain quarantined (NULL) until an operator can identify
    # their real owner. Every new device is created with a non-null owner.
    user_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    device_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(80), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    topics: Mapped[list[str]] = mapped_column(ARRAY(String))
    api_key_hash: Mapped[str] = mapped_column(String,nullable=False)
    status: Mapped[str] = mapped_column(String, default='offline')
    last_contact: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_in: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc)
    )

    plcs: Mapped[list["PLC"]] = relationship(
        back_populates="device",
        cascade="all, delete-orphan"
    )
    owner: Mapped[Optional[User]] = relationship(back_populates="devices")
    messages: Mapped[list[ReceivedMessage]] = relationship(back_populates="device")
    publications: Mapped[list[Publication]] = relationship(back_populates="device")

class PLC(Base):
    __tablename__ = "plcs"
    __table_args__ = (
        Index("index_plc_device_id", "device_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[str] = mapped_column(String, ForeignKey("devices.device_id"))
    name: Mapped[str] = mapped_column(String(80))
    description: Mapped[Optional[str]] = mapped_column(Text)
    ip: Mapped[str] = mapped_column(String(45))
    port_modbus: Mapped[int] = mapped_column(Integer, default=502)
    port_tcp: Mapped[int] = mapped_column(Integer, default=9000)
    protocol: Mapped[str] = mapped_column(String(20), default='modbus')
    unit_id: Mapped[int] = mapped_column(Integer, default=255)
    timeout: Mapped[float] = mapped_column(Float, default=5.0)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_in: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc)
    )

    device: Mapped[Device] = relationship(back_populates="plcs")
    registers: Mapped[list[MapRegister]] = relationship(
        back_populates="plc", cascade="all, delete-orphan"
    )

class MapRegister(Base):
    __tablename__ = "map_registers"
    __table_args__ = (
        UniqueConstraint("plc_id", "type", "address", name="uq_register_plc_type_address"),
        Index("index_register_plc_id", "plc_id"),
        Index("index_register,topic", "topic"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    plc_id: Mapped[int] = mapped_column(Integer, ForeignKey("plcs.id"))
    type: Mapped[str] = mapped_column(String(20))
    tag_name: Mapped[str] = mapped_column(String(30))
    address: Mapped[int] = mapped_column(Integer)
    topic: Mapped[str] = mapped_column(String)
    description: Mapped[Optional[str]] = mapped_column(Text)
    unit: Mapped[Optional[str]] = mapped_column(String(20))
    scale: Mapped[float] = mapped_column(Float, default=1.0)
    offset: Mapped[float] = mapped_column(Float, default=0.0)
    qos: Mapped[int] = mapped_column(Integer, default=1)
    read_only: Mapped[bool] = mapped_column(Boolean, default=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_in: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc)
    )

    plc: Mapped[PLC] = relationship(back_populates="registers")

    @property
    def address_modbus(self) -> int:
        """Endereço no padrão Mosbus clássico (com offset por tipo)."""

        return self.address + _OFFSET_MODBUS.get(self.type, 0)
    
class ReceivedMessage(Base):
    __tablename__ = "received_messages"
    __table_args__ = (
        Index("index_msg_device_id", "device_id"),
        Index("index_msg_topic", "topic"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("devices.device_id")
    )
    topic: Mapped[str] = mapped_column(String)
    payload: Mapped[Optional[str]] = mapped_column(Text)
    qos: Mapped[int] = mapped_column(Integer, default=0)
    retain: Mapped[bool] = mapped_column(Boolean, default=False)
    content_type: Mapped[Optional[str]] = mapped_column(String)
    user_props: Mapped[Optional[str]] = mapped_column(Text)
    received_in: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc)
    )

    device: Mapped[Optional[Device]] = relationship(back_populates="messages")

class Publication(Base):
    __tablename__ = "publications"
    __table_args__ = (
        Index("index_pub_mid", "mid"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    device_id: Mapped[Optional[str]] = mapped_column(
        String, ForeignKey("devices.device_id")
    )
    topic: Mapped[str] = mapped_column(String)
    payload: Mapped[Optional[str]] = mapped_column(Text)
    qos: Mapped[int] = mapped_column(Integer, default=0)
    mid: Mapped[Optional[int]] = mapped_column(Integer)
    confirmed_in: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True)
    )

    device: Mapped[Optional[Device]] = relationship(back_populates="publications")
