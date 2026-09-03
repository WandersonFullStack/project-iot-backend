"""Add device ownership and quarantine legacy devices.

Revision ID: 5b91f4c8d2a1
Revises: a61046df6047
Create Date: 2026-09-02

Existing devices cannot be assigned safely because the old schema did not
record their creator. They intentionally receive a NULL owner and become
invisible to normal users until an operator performs an audited backfill.
All application-created devices receive a non-null owner.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "5b91f4c8d2a1"
down_revision: Union[str, Sequence[str], None] = "a61046df6047"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "devices",
        sa.Column("user_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_devices_user_id_users",
        "devices",
        "users",
        ["user_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_devices_user_id",
        "devices",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_devices_user_id_active",
        "devices",
        ["user_id", "active"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_devices_user_id_active", table_name="devices")
    op.drop_index("ix_devices_user_id", table_name="devices")
    op.drop_constraint(
        "fk_devices_user_id_users",
        "devices",
        type_="foreignkey",
    )
    op.drop_column("devices", "user_id")
