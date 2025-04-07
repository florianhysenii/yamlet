import sys
import logging
from yamlet.config.pipeline_loader import load_pipeline_config
from yamlet.engine.spark_engine import SparkEngine

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def main(pipeline_path: str, action: str = "run"):
    """
    Command-line entry point for running or scheduling a pipeline.

    Args:
        pipeline_path (str): Path to the YAML pipeline configuration file.
        action (str): Action to perform: "run" or "schedule".
    """
    try:
        pipeline = load_pipeline_config(pipeline_path)
        logger.info("Pipeline '%s' loaded successfully.", pipeline.name)
    except Exception as e:
        logger.error("Failed to load pipeline: %s", e)
        raise

    spark_engine = SparkEngine(app_name=pipeline.name)
    spark_engine.start()
    logger.info("Spark engine started.")

    try:
        if action == "run":
            # 3) Run pipeline
            logger.info("Running pipeline '%s'.", pipeline.name)
            pipeline.run(spark_engine.spark)
        elif action == "schedule":
            # 4) Schedule pipeline
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
        logger.error("Usage: python -m yamlet.main <pipeline.yaml> [run|schedule]")
        sys.exit(1)

    pipeline_def = sys.argv[1]
    act = "run"
    if len(sys.argv) == 3:
        act = sys.argv[2]

    main(pipeline_def, act)
