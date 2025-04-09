import logging
import tempfile
import yaml
from fastapi import UploadFile, HTTPException, Query, APIRouter
from yamlet.config.pipeline_loader import load_pipeline_config
from yamlet.engine.spark_engine import SparkEngine
from yamlet.metadata.metadata_manager import PipelineMetadataManager

logger = logging.getLogger("api")
logger.setLevel(logging.INFO)

router = APIRouter()

@router.post("/pipelines/run")
async def run_pipeline(file: UploadFile):
    """
    Accepts a YAML file for a pipeline, loads and runs it immediately.
    """
    content = await file.read()
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".yaml") as tmp:
            tmp.write(content)
            temp_path = tmp.name
    except Exception as e:
        logger.error("Failed to write pipeline file: %s", e)
        raise HTTPException(status_code=500, detail="Could not save uploaded file.")

    try:
        pipeline = load_pipeline_config(temp_path)
        spark_engine = SparkEngine(app_name=pipeline.name)
        spark_engine.start()
        pipeline.run(spark_engine.spark)
    except Exception as e:
        logger.error("Pipeline execution failed: %s", e)
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        if 'spark_engine' in locals():
            spark_engine.stop()
            logger.info("Spark engine stopped after pipeline execution.")

    return {"status": "Pipeline executed successfully."}


@router.get("/pipelines/{pipeline_name}")
async def get_pipeline(pipeline_name: str, version: int = Query(None, description="Optional pipeline version number")):
    """
    Retrieves the pipeline YAML configuration and metadata for the specified pipeline.
    
    If no version is specified, returns the latest version.
    """
    try:
        metadata_manager = PipelineMetadataManager()
        record = metadata_manager.get_pipeline_config(pipeline_name, version)
        metadata_manager.close()
        if record is None:
            raise HTTPException(status_code=404, detail="Pipeline not found.")
        return record
    except Exception as e:
        logger.error("Error retrieving pipeline: %s", e)
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/pipelines/{pipeline_name}/versions")
async def get_pipeline_versions(pipeline_name: str):
    """
    Retrieves all versions of the specified pipeline from the metadata database.
    """
    try:
        metadata_manager = PipelineMetadataManager()
        versions = metadata_manager.get_pipeline_versions(pipeline_name)
        metadata_manager.close()
        if not versions:
            raise HTTPException(status_code=404, detail="Pipeline not found.")
        return {"versions": versions}
    except Exception as e:
        logger.error("Error retrieving pipeline versions: %s", e)
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/pipelines/save_version")
async def save_pipeline_version(file: UploadFile):
    """
    Saves the provided YAML pipeline configuration as a new version.

    Extracts the pipeline name from the YAML.
    """
    try:
        content = await file.read()
        yaml_str = content.decode("utf-8")
        pipeline_data = yaml.safe_load(yaml_str)
        pipeline_name = pipeline_data.get("name")
        if not pipeline_name:
            raise HTTPException(status_code=400, detail="Pipeline name is missing in the YAML.")
        metadata_manager = PipelineMetadataManager()
        version = metadata_manager.save_pipeline(pipeline_name, yaml_str)
        metadata_manager.close()
        return {"message": f"Pipeline '{pipeline_name}' saved as version {version}"}
    except Exception as e:
        logger.error("Error saving pipeline version: %s", e)
        raise HTTPException(status_code=400, detail=str(e))
