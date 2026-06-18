"""Add blacklisted flag to Stream

Revision ID: a1b2c3d4e5f6
Revises: b1345f835923
Create Date: 2026-05-20 11:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "b1345f835923"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("Stream", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "blacklisted",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            )
        )
    op.create_index("ix_stream_blacklisted", "Stream", ["blacklisted"])


def downgrade() -> None:
    op.drop_index("ix_stream_blacklisted", table_name="Stream")
    with op.batch_alter_table("Stream", schema=None) as batch_op:
        batch_op.drop_column("blacklisted")
