from __future__ import annotations
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from app.auth.user_auth import (
    create_access_token, generate_refresh_token,
    hash_refresh_token, verify_password, ACCESS_EXPIRE_MIN
)
from app.controllers.database_controller import DatabaseController
from app.auth.dependencies.depends import get_current_user, CurrentUser
from app.models.schema_users import LoginIn, TokenOut, AccessTokenOut, RefreshIn, UserOut

router = APIRouter(prefix="auth", tags=["Authentication"])

def get_db() -> DatabaseController:
    from app.main.main import db
    return db

DB = Annotated[DatabaseController, Depends(get_db)]

@router.post("/login", response_model=TokenOut, summary='Authenticates and issues tokens.')
def login(body: LoginIn, db: DB):
    """
    Valida username + senha e retorna access_token (JWT) + refresh_token (opaco).

    O access_token deve ser enviado em toda requisição subsequente:
        Authorization: Bearer <access_token>

    O refresh_token deve ser guardado com segurança pelo cliente
    (httpOnly cookie em SPAs, arquivo de config em CLIs).
    Nunca armazene o refresh_token em localStorage.
    """
    user = db.search_user_per_username(body.username)

    password_ok = verify_password(body.password, user["password_hash"]) if user else False

    if not user or not password_ok:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="username or password incorrect.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    if not user["active"]:
        raise HTTPException(
            status_code=403,
            detail="Conta desativada."
        )
    
    access = create_access_token(user["id"], user["username"], user["paper"])
    rt_plain, rt_hash = generate_refresh_token()

    db.create_refresh_token(user["id"], rt_hash)
    db.update_login_only(user["id"])

    return TokenOut(
        access_token=access,
        refresh_token=rt_plain,
        expires_in=ACCESS_EXPIRE_MIN
    )

@router.post("/refresh", response_model=AccessTokenOut, summary="Renew the access token.")
def refresh(body: RefreshIn, db: DB):
    """
    Usa o refresh_token para emitir um novo access_token sem nova autenticação.
    O refresh_token NÃO é rotacionado — o mesmo vale até expirar ou ser revogado.
    Para rotação automática (mais seguro), revogue e gere um novo a cada uso.
    """
    token_hash = hash_refresh_token(body.refresh_token)
    register = db.search_refresh_token(token_hash)

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
    
    user = db.search_users_per_id(register["user_id"])
    if not user or not user["active"]:
        raise HTTPException(
            status_code=401,
            detail="User inactive."
        )
    
    new_access = create_access_token(user["id"], user["username"], user["paper"])
    return AccessTokenOut(access_token=new_access)

@router.post(
    "/logout",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revokes the current refresh token."
)
def logout(body: RefreshIn, db: DB, user: CurrentUser):
    """
    Invalida o refresh_token fornecido.
    O access_token permanece válido até expirar naturalmente (por isso o prazo curto).
    Para invalidar todos os dispositivos, chame DELETE /auth/sessoes.
    """
    token_hash = hash_refresh_token(body.refresh_token)
    db.revoke_refresh_token(token_hash, user["id"])

@router.delete(
    "/sessions",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revokes all refresh tokens for the user (full logout)."
)
def full_logout(db: DB, user: CurrentUser):
    """Logout de todos os dispositivos — útil após suspeita de comprometimento."""
    db.revoke_all_refresh_tokens(user["id"])

@router.get(
    "/me",
    response_model=UserOut,
    summary="Returns the authenticated user."
)
def me(user: CurrentUser):
    return user
