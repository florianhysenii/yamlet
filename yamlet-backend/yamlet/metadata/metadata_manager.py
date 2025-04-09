import logging
from datetime import datetime
from sqlalchemy.orm import Session
from yamlet.database import SessionLocal
from yamlet.models.metadata_model import PipelineMetadata  # assuming this is your metadata model

logger = logging.getLogger(__name__)

class PipelineMetadataManager:
    """
    Manages pipeline metadata (e.g., pipeline configurations and versions) using SQLAlchemy.
    
    This class uses the SessionLocal from the database module so that connection details are
    defined in one place.
    """

    def __init__(self, db: Session = None):
        """
        Initialize the manager with a SQLAlchemy session.
        
        If no session is provided, a session is created from SessionLocal.
        """
        self.db = db or SessionLocal()

    def save_pipeline(self, pipeline_name: str, yaml_config: str) -> int:
        """
        Saves the pipeline YAML configuration as a new version, but only if there are changes.
        
        Args:
            pipeline_name (str): The name of the pipeline.
            yaml_config (str): The YAML configuration to save.
        
        Returns:
            int: The current version number after saving.
        """
        # Retrieve the latest version
        record = self.db.query(PipelineMetadata)\
            .filter(PipelineMetadata.pipeline_name == pipeline_name)\
            .order_by(PipelineMetadata.version.desc())\
            .first()

        # If the configuration hasn't changed, return the same version
        if record and record.yaml_config.strip() == yaml_config.strip():
            logger.info("No changes detected for pipeline '%s'. Keeping version %d.", pipeline_name, record.version)
            return record.version

        new_version = record.version + 1 if record else 1
        now = datetime.now()
        new_record = PipelineMetadata(
            pipeline_name=pipeline_name,
            version=new_version,
            yaml_config=yaml_config,
            created_at=now,
            updated_at=now
        )
        self.db.add(new_record)
        self.db.commit()
        self.db.refresh(new_record)
        logger.info("Saved pipeline '%s' as version %d.", pipeline_name, new_version)
        return new_version

    def get_pipeline_config(self, pipeline_name: str, version: int = None):
        """
        Retrieves the pipeline metadata for a given pipeline.
        
        Args:
            pipeline_name (str): The pipeline name.
            version (int, optional): Specific version number, or if omitted the latest version is returned.
        
        Returns:
            PipelineMetadata: The metadata record or None if not found.
        """
        if version is None:
            record = self.db.query(PipelineMetadata)\
                .filter(PipelineMetadata.pipeline_name == pipeline_name)\
                .order_by(PipelineMetadata.version.desc())\
                .first()
        else:
            record = self.db.query(PipelineMetadata)\
                .filter(PipelineMetadata.pipeline_name == pipeline_name, PipelineMetadata.version == version)\
                .first()
        return record

    def get_pipeline_versions(self, pipeline_name: str):
        """
        Retrieves all versions for the given pipeline.
        
        Args:
            pipeline_name (str): The pipeline name.
        
        Returns:
            List[PipelineMetadata]: A list of metadata records.
        """
        return self.db.query(PipelineMetadata)\
            .filter(PipelineMetadata.pipeline_name == pipeline_name)\
            .order_by(PipelineMetadata.version.desc())\
            .all()

    def close(self):
        """
        Closes the database session.
        """
        self.db.close()
