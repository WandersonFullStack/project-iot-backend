from __future__ import annotations
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.auth.user_auth import hash_password, verify_password
from app.controllers.database_controller import DatabaseController
from app.auth.dependencies.depends import AdminUser, CurrentUser, get_current_user
from app.models.schema_users import UserIn, UserOut, UserUpdate, ReplacePasswordIn
from app.models.schemas import PagesParams

router = APIRouter(prefix="/api/v1/users", tags=["Users"])

def get_db() -> DatabaseController:
    from app.main.main import db
    return db

DB = Annotated[DatabaseController, Depends(get_db)]

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
    summary="create a new user (admin)"
)
def create_user(body: UserIn, db: DB, _: AdminUser):
    """Apenas admins podem criar usuários"""

    try:
        user_id = db.create_user(
            username=body.username,
            email=body.email,
            name=body.name,
            password_hash=hash_password(body.password),
            paper=body.paper,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e)
        )
    
    return dict(db.search_users_per_id(user_id))

@router.get(
    "",
    response_model=list[UserOut],
    summary="List users (admin)"
)
def list_users(db: DB, pag: Pag, _: AdminUser):
    return [dict(r) for r in db.list_users(pag.limit, pag.offset)]

@router.get(
    "/{user_id}",
    response_model=UserOut,
    summary="Search user (admin)"
)
def search_user(user_id: int, db: DB, _: AdminUser):
    row = db.search_users_per_id(user_id)

    if not row:
        raise HTTPException(
            status_code=404, detail="User not found."
        )
    
    return dict(row)

@router.patch(
    "/{user_id}",
    response_model=UserOut,
    summary="updated user (admin)"
)
def update_user(user_id: int, body: UserUpdate, db: DB, _: AdminUser):
    if not db.search_users_per_id(user_id):
        raise HTTPException(
            status_code=404,
            detail="User not found."
        )
    db.update_user(
        user_id,
        **{k: v for k, v in body.model_dump().items() if v is not None}
    )

    return dict(db.search_users_per_id(user_id))

@router.post(
    "/{user_id}/change-password",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Change a user's password."
)
def change_password(
    user_id: int,
    body: ReplacePasswordIn,
    db: DB,
    logged_in_user: CurrentUser
):
    """
    Regras de acesso:
      - Admin pode trocar a senha de qualquer usuário sem confirmar a atual.
      - Usuário comum só pode trocar a própria senha, confirmando a atual.

    Após troca de senha, todos os refresh tokens são revogados — forçando
    re-autenticação em todos os dispositivos.
    """
    target = db.search_users_per_id(user_id)

    if not target:
        raise HTTPException(
            status_code=404,
            detail="User not found."
        )
    
    eh_admin = logged_in_user["paper"] == "admin"
    eh_own = logged_in_user["id"] == user_id

    if not eh_admin and not eh_own:
        raise HTTPException(
            status_code=403,
            detail="No permission to modify this user."
        )
    
    # Usuário comum deve confirmar a senha atual
    if not eh_admin:
        if not verify_password(body.current_password, target["password_hash"]):
            raise HTTPException(
                status_code=401,
                detail="Current password is incorrect."
            )
        
    db.update_user(user_id, password_hash=hash_password(body.new_password))
    db.revoke_all_refresh_tokens(user_id)
    