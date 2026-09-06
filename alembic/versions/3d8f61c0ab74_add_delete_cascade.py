"""Enforce delete cascade across the ownership chain.

Revision ID: 3d8f61c0ab74
Revises: 5b91f4c8d2a1
Create Date: 2026-09-05

A migration inicial criou as chaves estrangeiras sem ON DELETE, e
devices.user_id foi criada como RESTRICT. Isso impedia tanto a remocao
de um PLC com registradores quanto a remocao da conta de um usuario.

Reconstruimos as seis FKs da cadeia de propriedade com ON DELETE CASCADE.
"""

from typing import Sequence, Union

from alembic import op


revision: str = "3d8f61c0ab74"
down_revision: Union[str, Sequence[str], None] = "5b91f4c8d2a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# (tabela_filha, coluna, tabela_pai, coluna_pai, nome_da_constraint)
_FOREIGN_KEYS = (
    ("refresh_tokens", "user_id", "users", "id",
     "fk_refresh_tokens_user_id_users"),
    ("devices", "user_id", "users", "id",
     "fk_devices_user_id_users"),
    ("plcs", "device_id", "devices", "device_id",
     "fk_plcs_device_id_devices"),
    ("map_registers", "plc_id", "plcs", "id",
     "fk_map_registers_plc_id_plcs"),
    ("received_messages", "device_id", "devices", "device_id",
     "fk_received_messages_device_id_devices"),
    ("publications", "device_id", "devices", "device_id",
     "fk_publications_device_id_devices"),
)


def _drop_existing_fk(table: str, column: str) -> None:
    """
    A migration inicial nao nomeou as FKs, entao o Postgres gerou nomes
    automaticos. Descobrimos o nome real em pg_constraint em vez de assumi-lo,
    o que mantem a migration reexecutavel em bancos criados por versoes
    diferentes do schema.
    """
    op.execute(
        f"""
        DO $$
        DECLARE
            fk_name text;
        BEGIN
            SELECT con.conname INTO fk_name
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_attribute att
              ON att.attrelid = con.conrelid AND att.attnum = con.conkey[1]
            WHERE con.contype = 'f'
              AND rel.relname = '{table}'
              AND att.attname = '{column}'
              AND array_length(con.conkey, 1) = 1;

            IF fk_name IS NOT NULL THEN
                EXECUTE format(
                    'ALTER TABLE %I DROP CONSTRAINT %I', '{table}', fk_name
                );
            END IF;
        END $$;
        """
    )


def upgrade() -> None:
    for child, column, parent, parent_column, name in _FOREIGN_KEYS:
        _drop_existing_fk(child, column)
        op.create_foreign_key(
            name, child, parent, [column], [parent_column],
            ondelete="CASCADE",
        )


def downgrade() -> None:
    for child, column, parent, parent_column, name in _FOREIGN_KEYS:
        op.drop_constraint(name, child, type_="foreignkey")
        op.create_foreign_key(
            name, child, parent, [column], [parent_column],
            ondelete="RESTRICT" if child == "devices" else None,
        )