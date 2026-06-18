"""Per-service 451 list on Stream

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-05-20 14:30:00.000000

Renames the boolean Stream.blacklisted introduced by patch 0007 to a
JSON list Stream.flagged_451_services holding the keys of every
debrid service that has 451'd this hash. Backfills any row with the
old boolean = True to ["realdebrid"] (the only debrid we had then).

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("Stream", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "flagged_451_services",
                sa.JSON(),
                nullable=False,
                server_default=sa.text("'[]'"),
            )
        )
    op.execute(
        """
        UPDATE "Stream"
        SET flagged_451_services = '["realdebrid"]'
        WHERE blacklisted = true
        """
    )
    op.drop_index("ix_stream_blacklisted", table_name="Stream")
    with op.batch_alter_table("Stream", schema=None) as batch_op:
        batch_op.drop_column("blacklisted")


def downgrade() -> None:
    with op.batch_alter_table("Stream", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "blacklisted",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            )
        )
    op.execute(
        """
        UPDATE "Stream"
        SET blacklisted = true
        WHERE flagged_451_services::text LIKE '%realdebrid%'
        """
    )
    op.create_index("ix_stream_blacklisted", "Stream", ["blacklisted"])
    with op.batch_alter_table("Stream", schema=None) as batch_op:
        batch_op.drop_column("flagged_451_services")
