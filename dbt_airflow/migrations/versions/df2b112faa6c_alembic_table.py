"""alembic tablme

Revision ID: df2b112faa6c
Revises: 1d3965376df9
Create Date: 2023-06-01 10:54:00.656032

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'df2b112faa6c'
down_revision = '1d3965376df9'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "dbt_jos",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("job_id", sa.VARCHAR, unique=True, nullable=False),
        sa.Column("model_name", sa.VARCHAR, unique=False, nullable=False),
        sa.Column("test_or_run", sa.VARCHAR, unique=False, nullable=False),
        sa.Column("status", sa.VARCHAR, nullable=False),
        sa.Column("c_date", sa.DateTime, nullable=True),
        schema='public'
    )


def downgrade():
    pass
