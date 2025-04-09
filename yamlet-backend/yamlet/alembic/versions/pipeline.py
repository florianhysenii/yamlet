"""Create connections and pipeline_metadata tables

Revision ID: abcdef123456
Revises: 
Create Date: 2025-04-09 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func

# revision identifiers, used by Alembic.
revision = 'abcdef123456'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # Create the connections table.
    op.create_table(
        'connections',
        sa.Column('id', sa.Integer, primary_key=True, index=True),
        sa.Column('name', sa.String(255), unique=True, nullable=False),
        sa.Column('type', sa.String(50), nullable=False),
        sa.Column('host', sa.String(255), nullable=False),
        sa.Column('port', sa.Integer, nullable=False),
        sa.Column('database', sa.String(255), nullable=False),
        sa.Column('user', sa.String(255), nullable=False),
        sa.Column('password', sa.String(255), nullable=False),
    )
    
    # Create the pipeline_metadata table.
    op.create_table(
        'pipeline_metadata',
        sa.Column('id', sa.Integer, primary_key=True, index=True),
        sa.Column('pipeline_name', sa.String(255), nullable=False),
        sa.Column('version', sa.Integer, nullable=False),
        sa.Column('yaml_config', sa.Text, nullable=False),
        sa.Column('created_at', sa.DateTime, server_default=func.now()),
        sa.Column('updated_at', sa.DateTime, server_default=func.now(), onupdate=func.now()),
    )


def downgrade():
    op.drop_table('pipeline_metadata')
    op.drop_table('connections')

#alembic upgrade head
