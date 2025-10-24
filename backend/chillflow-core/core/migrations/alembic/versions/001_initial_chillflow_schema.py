"""Initial ChillFlow schema

Revision ID: 001
Revises:
Create Date: 2025-01-24 16:50:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create initial ChillFlow database schema."""

    # Create schemas
    op.execute("CREATE SCHEMA IF NOT EXISTS dim;")
    op.execute("CREATE SCHEMA IF NOT EXISTS stg;")
    op.execute("CREATE SCHEMA IF NOT EXISTS mart;")

    # Create dim.zone table
    op.create_table(
        "zone",
        sa.Column("zone_id", sa.Integer(), nullable=False),
        sa.Column("borough", sa.String(length=50), nullable=False),
        sa.Column("zone_name", sa.String(length=100), nullable=False),
        sa.Column("service_zone", sa.String(length=50), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("zone_id"),
        schema="dim",
    )

    # Create stg.complete_trip table
    op.create_table(
        "complete_trip",
        sa.Column("trip_key", sa.String(length=64), nullable=False),
        sa.Column("vendor_id", sa.Integer(), nullable=False),
        sa.Column("pickup_ts", sa.TIMESTAMP(), nullable=False),
        sa.Column("dropoff_ts", sa.TIMESTAMP(), nullable=False),
        sa.Column("pu_zone_id", sa.Integer(), nullable=False),
        sa.Column("do_zone_id", sa.Integer(), nullable=False),
        sa.Column("passenger_count", sa.Integer(), nullable=True),
        sa.Column("trip_distance", sa.Float(), nullable=True),
        sa.Column("fare_amount", sa.Float(), nullable=True),
        sa.Column("tip_amount", sa.Float(), nullable=True),
        sa.Column("total_amount", sa.Float(), nullable=True),
        sa.Column("payment_type", sa.Integer(), nullable=True),
        sa.Column("vehicle_id_h", sa.String(length=16), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("trip_key"),
        sa.ForeignKeyConstraint(
            ["pu_zone_id"],
            ["dim.zone.zone_id"],
        ),
        sa.ForeignKeyConstraint(
            ["do_zone_id"],
            ["dim.zone.zone_id"],
        ),
        schema="stg",
    )

    # Create indexes for performance
    op.create_index(
        "idx_complete_trip_pickup_ts", "complete_trip", ["pickup_ts"], schema="stg"
    )
    op.create_index(
        "idx_complete_trip_pu_zone", "complete_trip", ["pu_zone_id"], schema="stg"
    )
    op.create_index(
        "idx_complete_trip_do_zone", "complete_trip", ["do_zone_id"], schema="stg"
    )
    op.create_index(
        "idx_complete_trip_vendor", "complete_trip", ["vendor_id"], schema="stg"
    )


def downgrade() -> None:
    """Drop initial ChillFlow database schema."""

    # Drop indexes
    op.drop_index("idx_complete_trip_vendor", table_name="complete_trip", schema="stg")
    op.drop_index("idx_complete_trip_do_zone", table_name="complete_trip", schema="stg")
    op.drop_index("idx_complete_trip_pu_zone", table_name="complete_trip", schema="stg")
    op.drop_index(
        "idx_complete_trip_pickup_ts", table_name="complete_trip", schema="stg"
    )

    # Drop tables
    op.drop_table("complete_trip", schema="stg")
    op.drop_table("zone", schema="dim")

    # Drop schemas
    op.execute("DROP SCHEMA IF EXISTS mart CASCADE;")
    op.execute("DROP SCHEMA IF EXISTS stg CASCADE;")
    op.execute("DROP SCHEMA IF EXISTS dim CASCADE;")
