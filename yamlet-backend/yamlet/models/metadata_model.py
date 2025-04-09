from sqlalchemy import Column, Integer, String, Text, DateTime, func
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class PipelineMetadata(Base):
    """
    SQLAlchemy model for storing ETL pipeline metadata (configuration versions).

    Attributes:
        id (int): Primary key.
        pipeline_name (str): Name of the pipeline.
        version (int): Version number of the pipeline configuration.
        yaml_config (str): The YAML configuration for the pipeline.
        created_at (datetime): Timestamp when the record was created.
        updated_at (datetime): Timestamp when the record was last updated.
    """
    __tablename__ = "pipeline_metadata"

    id = Column(Integer, primary_key=True, index=True, doc="Primary key of the metadata record")
    pipeline_name = Column(String(255), nullable=False, doc="Name of the pipeline")
    version = Column(Integer, nullable=False, doc="Version number")
    yaml_config = Column(Text, nullable=False, doc="YAML configuration of the pipeline")
    created_at = Column(DateTime, server_default=func.now(), doc="Record creation timestamp")
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), doc="Record last updated timestamp")
