import hashlib
import secrets
from typing import Annotated, AsyncGenerator

from fastapi import Header, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import AsyncSessionLocal

def generate_api_key() -> str:
    """Gera uma chave aleatória criptograficamente segura de 32 bytes(256 bits)."""
    return secrets.token_urlsafe(32)

def hash_api_key(api_key: str) -> str:
    """Gera o hash SHA-256 da api_key para armazenamento."""
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()

def check_api_key(api_key_plain: str, api_key_hash: str) -> bool:
    """Compara o hash da chave fornecida com o hash armazenado."""
    return hash_api_key(api_key_plain) == api_key_hash

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

DB = Annotated[AsyncSession, Depends(get_db)]

async def authenticate_device(
        x_device_id: str = Header(..., description="device_id do dispositivo"),
        x_api_key: str = Header(..., description="api_key emitida no registro"),
        db: DB = None
) -> dict:
    """Dependência FastAPI para rotas que exigem autenticação de dispositivos."""
    device = db.search_device(x_device_id)
    if not device or not device["active"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Device not found or inactive.",
            headers={"WWW-Authenticate": "X-Api-Key"}
        )
    if not check_api_key(x_api_key, device["api_key_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="api_key invalidates.",
            headers={"WWW-Authenticate": "X-Api-Key"}
        )
    return dict(device)

