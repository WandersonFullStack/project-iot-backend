from __future__ import annotations
from datetime import datetime, timezone
from typing import Annotated, AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.controllers import user_controller as uc
from app.auth.user_auth import (
    create_access_token, generate_refresh_token,
    hash_refresh_token, verify_password, ACCESS_EXPIRE_MIN
)
from app.config.database import AsyncSessionLocal
from app.auth.dependencies.depends import CurrentUser
from app.models.schema_users import LoginIn, TokenOut, AccessTokenOut, RefreshIn, UserOut

router = APIRouter(prefix="/auth", tags=["Authentication"])

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

DB = Annotated[AsyncSession, Depends(get_db)]

@router.post("/login", response_model=TokenOut, summary='Authenticates and issues tokens.')
async def login(body: LoginIn, db: DB):
    """
    Valida username + senha e retorna access_token (JWT) + refresh_token (opaco).

    O access_token deve ser enviado em toda requisição subsequente:
        Authorization: Bearer <access_token>

    O refresh_token deve ser guardado com segurança pelo cliente
    (httpOnly cookie em SPAs, arquivo de config em CLIs).
    Nunca armazene o refresh_token em localStorage.
    """
    user = await uc.search_user_by_username(db, body.username)

    password_ok = verify_password(body.password, user.password_hash) if user else False

    if not user or not password_ok:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="username or password incorrect.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not user.active:
        raise HTTPException(
            status_code=403,
            detail="Conta desativada."
        )
    
    access = create_access_token(db, user.id, user.username)
    rt_plain, rt_hash = generate_refresh_token()

    await uc.create_refresh_token(db, user.id, rt_hash)
    # await uc.update_login_only(db, user.id)

    await db.commit()

    return TokenOut(
        access_token=access,
        refresh_token=rt_plain,
        expires_in=ACCESS_EXPIRE_MIN
    )

@router.post("/refresh", response_model=AccessTokenOut, summary="Renew the access token.")
async def refresh(body: RefreshIn, db: DB):
    """
    Usa o refresh_token para emitir um novo access_token sem nova autenticação.
    O refresh_token NÃO é rotacionado — o mesmo vale até expirar ou ser revogado.
    Para rotação automática (mais seguro), revogue e gere um novo a cada uso.
    """
    token_hash = await hash_refresh_token(db, body.refresh_token)
    register = await uc.search_refresh_token(db, token_hash)

    if not register:
        raise HTTPException(
            status_code=401,
            detail="Refresh token is invalid or revoked."
        )
    
    if datetime.fromisoformat(register["expires_in"]) < datetime.now(timezone.utc):
        raise HTTPException(
            status_code=401,
            detail="Refresh token expired. Please log in again."
        )
    
    user = await uc.search_user_by_id(db, register.user_id)
    if not user or not user.active:
        raise HTTPException(
            status_code=401,
            detail="User inactive."
        )
    
    new_access = await create_access_token(db, user.id, user.username)

    await db.commit()

    return AccessTokenOut(
        access_token=new_access
    )

@router.post(
    "/logout",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revokes the current refresh token."
)
async def logout(body: RefreshIn, db: DB, user: CurrentUser):
    """
    Invalida o refresh_token fornecido.
    O access_token permanece válido até expirar naturalmente (por isso o prazo curto).
    Para invalidar todos os dispositivos, chame DELETE /auth/sessoes.
    """
    token_hash = await hash_refresh_token(db, body.refresh_token)
    await uc.revoke_refresh_token(db, token_hash, user.id)

    await db.commit()

@router.delete(
    "/sessions",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revokes all refresh tokens for the user (full logout)."
)
async def full_logout(db: DB, user: CurrentUser):
    """Logout de todos os dispositivos — útil após suspeita de comprometimento."""
    await uc.revoked_all_refresh_tokens(db, user.id)

    await db.commit()

@router.get(
    "/me",
    response_model=UserOut,
    summary="Returns the authenticated user."
)
async def me(user: CurrentUser):
    
    return user
