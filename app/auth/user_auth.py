from __future__ import annotations
import hashlib
import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone

from jose import JWTError, jwt
from passlib.context import CryptContext

SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = os.getenv("JWT_ALGORITHM")

ACCESS_EXPIRE_MIN = int(os.getenv("ACCESS_EXPIRE_MIN"))
REFRESH_EXPIRE_DAYS = int(os.getenv("REFRESH_EXPIRE_DAYS"))

pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(password: str) -> str:
    return pwd_ctx.hash(password)

def verify_password(password: str, hash_stored: str) -> bool:
    return pwd_ctx.verify(password, hash_stored)

def create_access_token(user_id: int, username: str, paper: str) -> str:
    """Gera um JWT assinado com HS256."""

    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "username": username,
        "paper": paper,
        "exp": now + timedelta(minutes=ACCESS_EXPIRE_MIN),
        "jti": str(uuid.uuid4)
    }

    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def decode_token() -> tuple[str, str]:
    """
    Gera um token opaco (não-JWT) de alta entropia.

    Retorna (token_plain, token_hash):
      token_plain → enviado ao cliente, nunca armazenado
      token_hash  → SHA-256 armazenado no banco
    """
    token = secrets.token_urlsafe(64)
    token_hash = hashlib.sha256(token.encode()).hexdigest()

    return token, token_hash

def hash_refresh_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()
