from __future__ import annotations
from typing import Annotated, AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import AsyncSessionLocal
from app.auth.user_auth import hash_password, verify_password
from app.controllers import user_controller as uc
from app.auth.dependencies.depends import CurrentUser
from app.models.schema_users import (
    UserIn, UserOut, UserUpdate, 
    ReplacePasswordIn, DeleteAccountIn
)
from app.models.schemas import PagesParams

router = APIRouter(prefix="/api/v1/users", tags=["Users"])

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session

DB = Annotated[AsyncSession, Depends(get_db)]

def _page_params(
        limit: int = Query(20, ge=1, le=100),
        offset: int = Query(0, ge=0)
) -> PagesParams:
    return PagesParams(limit=limit, offset=offset)

Pag = Annotated[PagesParams, Depends(_page_params)]

@router.post(
    "",
    response_model=UserOut,
    status_code=status.HTTP_201_CREATED,
    summary="create a new user"
)
async def create_user(body: UserIn, db: DB):
 
    try:
        user = await uc.create_user(
            db,
            username=body.username,
            email=body.email,
            name=body.name,
            password_hash=hash_password(body.password),
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    await db.commit()

    return user

@router.patch(
    "/{user_id}",
    response_model=UserOut,
    summary="updated user"
)
async def update_user(user_id: int, body: UserUpdate, db: DB, user: CurrentUser):
    if user.id != user_id:
        raise HTTPException(
            status_code=404,
            detail="User not found."
        )
    await uc.update_user(
        db,
        user_id,
        **{k: v for k, v in body.model_dump().items() if v is not None}
    )
    await db.commit()

    current_user = await uc.search_user_by_id(db, user_id)

    return current_user

@router.post(
    "/{user_id}/change-password",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Change a user's password."
)
async def change_password(
    user_id: int,
    body: ReplacePasswordIn,
    db: DB,
    logged_in_user: CurrentUser
):
    """
    Após troca de senha, todos os refresh tokens são revogados — forçando
    re-autenticação em todos os dispositivos.
    """
    if logged_in_user.id != user_id:
        raise HTTPException(
            status_code=404,
            detail="User not found."
        )

    if not verify_password(
        body.current_password,
        logged_in_user.password_hash,
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Current password is incorrect.",
        )
    
    await uc.update_user(
        db, 
        user_id, 
        password_hash=hash_password(
            body.new_password
        )
    )

    await uc.revoked_all_refresh_tokens(db, user_id)
        
    await db.commit()


# -> ME = User Profile

@router.get(
    "/me/profile",
    response_model=UserOut,
    summary="Get current user profole"
)
async def get_profile(
    db: DB,
    user: CurrentUser
):
    return user

@router.post(
    "/me/change-password",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Change own password"
)
async def chande_own_password(
    body: ReplacePasswordIn,
    db: DB,
    user: CurrentUser
):
    """Usuário muda sua própria senha, confirmando a atual."""

    if not verify_password(body.current_password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Current password is incrrect."
        )
    
    await uc.update_user(
        db,
        user.id,
        password_hash=hash_password(
            body.new_password
        )
    )
    await uc.revoked_all_refresh_tokens(
        db,
        user.id
    )
    await db.commit()

@router.delete(
    "/me",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Permanently delete own account and all associated data"
)
async def delete_own_account(
    body: DeleteAccountIn,
    db: DB,
    user: CurrentUser,
):
    """
    Exige a senha atual como confirmação. Remove em cascata os refresh-tokens,
    os dispositivos do usuário e, sob cada dispositivo, o PLC, os
    registradores e todo o histórico de menssagens e publicações.

    Os tokens de acesso ja emitidos param de funcionar imediatamente porque
    `get_account_user` revalida a existência do usuário no banco
    a cada requisição. Operação irreversivel.
    """
    if not varify_password(body.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Current password is incorrect.",
        )

    await uc.delete_user(db, user_id)
    await db.commit()