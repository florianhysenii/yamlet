# In alembic/env.py
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
fileConfig(config.config_file_name)

# Add your model's MetaData object here for 'autogenerate' support
from yamlet.models.connections_model import Base as ConnectionsBase
from yamlet.models.metadata_model import Base as MetadataBase

# Combine metadata from both models (if they're defined separately)
target_metadata = ConnectionsBase.metadata.union(MetadataBase.metadata)

# ... rest of env.py setup
