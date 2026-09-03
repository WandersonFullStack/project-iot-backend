from __future__ import annotations
from typing import Annotated, AsyncGenerator

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError

from sqlalchemy.ext.asyncio import AsyncSession

from ..user_auth import decode_token
from app.config.database import AsyncSessionLocal
from app.controllers import user_controller as uc
from app.models.schema_orm import User

_bearer = HTTPBearer(auto_error=True)

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

DB = Annotated[AsyncSession, Depends(get_db)]

async def get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(_bearer),
        db: AsyncSession = Depends(get_db)
) -> User:
    """
    Dependência principal de autenticação.

    Fluxo:
      1. Extrai o token do header "Authorization: Bearer <token>"
      2. Decodifica e valida assinatura + expiração via jose
      3. Confirma que o usuário ainda existe e está ativo no banco
         (garante que usuários desativados percam acesso mesmo com token válido)

    Lança 401 em qualquer falha — nunca 403 (não revela se o recurso existe).
    """
    try:
        payload = decode_token(credentials.credentials)
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    user_id = payload.get("sub")
    try:
        user_id = int(user_id)
    except (ValueError, TypeError):
        raise HTTPException(
            status_code=401,
            detail="Malformed token."
        )
    
    user = await uc.search_user_by_id(db, user_id)
    if not user or not user.active:
        raise HTTPException(
            status_code=401,
            detail="User not found or anactive."
        )
    
    return user



# Aliases tipados -> assinatura das rotas
CurrentUser = Annotated[User, Depends(get_current_user)]
