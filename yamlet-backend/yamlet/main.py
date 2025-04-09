# my_etl_project/main.py
import sys
import logging
from yamlet.config.pipeline_loader import load_pipeline_config
from yamlet.engine.spark_engine import SparkEngine
from yamlet.metadata.metadata_manager import PipelineMetadataManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def main(pipeline_path: str, action: str = "run"):
    """
    Command-line entry point for running or scheduling a pipeline.
    
    Args:
        pipeline_path (str): Path to YAML pipeline configuration.
        action (str): "run" or "schedule"
    """
    try:
        pipeline = load_pipeline_config(pipeline_path)
        logger.info("Pipeline '%s' loaded successfully.", pipeline.name)
    except Exception as e:
        logger.error("Failed to load pipeline: %s", e)
        raise

    # Initialize Spark engine
    spark_engine = SparkEngine(app_name=pipeline.name)
    spark_engine.start()
    logger.info("Spark engine started.")

    try:
        if action == "run":
            logger.info("Running pipeline '%s'.", pipeline.name)
            pipeline.run(spark_engine.spark)
            
            # Read the YAML configuration from file for metadata saving.
            with open(pipeline_path, "r", encoding="utf-8") as f:
                yaml_config = f.read()
            
            # Save pipeline metadata (update connection details accordingly)
            metadata_manager = PipelineMetadataManager(
                host="localhost",
                user="root",
                password="password",
                database="testdb"
            )
            version = metadata_manager.save_pipeline(pipeline.name, yaml_config)
            logger.info("Pipeline '%s' saved as version %d.", pipeline.name, version)
            metadata_manager.close()
        elif action == "schedule":
            logger.info("Scheduling pipeline '%s'.", pipeline.name)
            pipeline.schedule_pipeline(pipeline_id=pipeline.name)
        else:
            logger.error("Unknown action: %s", action)
            raise ValueError(f"Unknown action: {action}")
    except Exception as e:
        logger.error("Pipeline execution error: %s", e)
    finally:
        spark_engine.stop()
        logger.info("Spark engine stopped.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python -m my_etl_project.main <pipeline.yaml> [run|schedule]")
        sys.exit(1)
    
    pipeline_def = sys.argv[1]
    act = "run"
    if len(sys.argv) == 3:
        act = sys.argv[2]
    
    main(pipeline_def, act)
