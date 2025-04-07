import logging
from fastapi import FastAPI, UploadFile, HTTPException
from yamlet.config.pipeline_loader import load_pipeline_config
from yamlet.engine.spark_engine import SparkEngine

logger = logging.getLogger("api")
logger.setLevel(logging.INFO)

app = FastAPI(
    title="My ETL API",
    description="API to run ETL pipelines via YAML configuration.",
    version="1.0.0"
)

@app.post("/pipelines/run")
async def run_pipeline(file: UploadFile):
    """
    Accepts a YAML file for a pipeline, loads and runs it immediately.
    """
    content = await file.read()
    temp_path = "/tmp/pipeline.yaml"
    
    try:
        with open(temp_path, "wb") as f:
            f.write(content)
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
