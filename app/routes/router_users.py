from __future__ import annotations
from typing import Annotated, AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import AsyncSessionLocal
from app.auth.user_auth import hash_password, verify_password
from app.controllers import user_controller as uc
from app.auth.dependencies.depends import CurrentUser
from app.models.schema_users import UserIn, UserOut, UserUpdate, ReplacePasswordIn
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
        user_id = await uc.create_user(
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
    
    return user_id


@router.patch(
    "/{user_id}",
    response_model=UserOut,
    summary="updated user"
)
async def update_user(user_id: int, body: UserUpdate, db: DB, _: CurrentUser):
    user = await uc.search_user_by_username(db, user_id)

    if not user:
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

    return user

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
    target = await uc.search_user_by_id(db, user_id)

    if not target:
        raise HTTPException(
            status_code=404,
            detail="User not found."
        )
    
    await uc.update_user(db, user_id, password_hash=hash_password(body.new_password))
    await uc.revoked_all_refresh_tokens(db, user_id)
        
    await db.commit()