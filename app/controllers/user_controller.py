from __future__ import annotations
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update, delete, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema_orm import User, RefreshToken

async def create_user(
        db: AsyncSession,
        username: str,
        email: str,
        name: str,
        password_hash: str,
) -> User:
    user = User(
        username=username,
        email=email,
        name=name,
        password_hash=password_hash
    )
    db.add(user)

    try:
        await db.flush()
    except IntegrityError:
        await db.rollback()
        raise ValueError("username or email already registered")
    return user

async def search_user_by_id(
        db: AsyncSession,
        user_id: int
) -> User | None:
    result = await db.execute(select(User).where(User.id == user_id))

    return result.scalar_one_or_none()

async def search_user_by_username(
        db: AsyncSession,
        username: str
) -> User | None:
    result = await db.execute(select(User).where(User.username == username))

    return result.scalar_one_or_none()

async def list_users(
        db: AsyncSession,
        limit: int = 50,
        offset: int = 0,
) -> list[User]:
    result = await db.execute(
        select(User).order_by(User.name).limit(limit).offset(offset)
    )

    return list(result.scalars().all())

async def update_user(
        db: AsyncSession,
        user_id: int,
        **fields
) -> bool:
    allowed = {"name", "email", "active", "password_hash"}
    values = {k: v for k, v in fields.items() if k in allowed and v is not None}

    if not values:
        return False
    
    result = await db.execute(
        update(User).where(User.id == user_id).values(**values)
    )

    return result.rowcount > 0

async def update_login_only(
        db: AsyncSession,
        user_id: int
) -> None:
    await db.execute(
        update(User)
        .where(User.id == user_id)
        .values(login_only=datetime.now(timezone.utc))
    )

async def count_users(db: AsyncSession) -> int:
    result = await db.execute(select(func.count(User.id)))

    return result.scalar_one()

async def create_refresh_token(
        db: AsyncSession,
        user_id: int,
        token_hash: str,
        days: int = 7,
) -> RefreshToken:
    token = RefreshToken(
        user_id=user_id,
        token_hash=token_hash,
        expires_in=datetime.now(timezone.utc) + timedelta(days=days),
    )

    db.add(token)
    await db.flush()
    
    return token

async def search_refresh_token(
        db: AsyncSession,
        token_hash: str
) -> RefreshToken | None:
    result = await db.execute(
        select(RefreshToken).where(
            RefreshToken.token_hash == token_hash,
            RefreshToken.revoked == False,
        )
    )

    return result.scalar_one_or_none()

async def revoke_refresh_token(
        db: AsyncSession,
        token_hash: str,
        user_id: int,
) -> bool:
    result = await db.execute(
        update(RefreshToken)
        .where(RefreshToken.token_hash == token_hash, RefreshToken.user_id == user_id)
        .values(revoked=True)
    )

    return result.rowcount > 0

async def revoked_all_refresh_tokens(
        db: AsyncSession,
        user_id: int,
) -> None:
    await db.execute(
        update(RefreshToken)
        .where(RefreshToken.user_id == user_id)
        .values(revoked=True)
    )

async def clean_expired_refresh_tokens(
        db: AsyncSession
) -> None:
    """Manutenção periódica — chamada pela tarefa de background."""

    await db.execute(
        delete(RefreshToken).where(
            (RefreshToken.expires_in < datetime.now(timezone.utc))
            | (RefreshToken.revoked == True)
        )
    )
    