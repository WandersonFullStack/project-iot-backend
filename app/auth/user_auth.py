from __future__ import annotations
import hashlib
import os
import secrets
import uuid

from jose import JWTError, jwt
import bcrypt

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "JWT_SECRET_KEY")
if not SECRET_KEY:
    raise ReferenceError("JWT_SECRET_KEY enviroment variable is not set.")
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

ACCESS_EXPIRE_MIN = int(os.getenv("ACCESS_EXPIRE_MIN", 30))
REFRESH_EXPIRE_DAYS = int(os.getenv("REFRESH_EXPIRE_DAYS", 7))

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password: str, hash_stored: str) -> bool:
    return bcrypt.checkpw(password.encode(), hash_stored.encode())

def create_access_token(user_id: int, username: str) -> str:
    """Gera um JWT assinado com HS256."""

    payload = {
        "sub": str(user_id),
        "username": username,
        "jti": str(uuid.uuid4())
    }

    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> dict:
    """
    Valida assinatura, algoritmo e expiração.
    Lança jose.JWTError em qualquer falha -> nunca retorna payload invalido.
    """
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

def generate_refresh_token() -> tuple[str, str]:
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
