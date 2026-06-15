from __future__ import annotations
import asyncio
import logging
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import context

# Importar todos os modelos ORM
from app.config.database import Base
from app.models.schema_orm import (
    User, RefreshToken, Device, PLC, MapRegister,
    ReceivedMessage, Publication
)

import os
from urllib.parse import quote_plus

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://mqttgateway:mqttdev@localhost:5434/system-iot-db"
)

# Configuração de log
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)
logger = logging.getLogger('alembic.env')

# Alvo de metadados para autogenerate
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """
    Executa migrações em modo offline.
    Útil para gerar SQL sem conexão real ao banco.
    """
    sqlalchemy_url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=sqlalchemy_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    """Executa migrações com a conexão fornecida."""
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """
    Executa migrações em modo online.
    Cria uma engine assíncrona e executa as migrações.
    """

    # Criar engine assíncrona
    connectable = create_async_engine(
        DATABASE_URL,
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        # Executar dentro de uma transação
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())