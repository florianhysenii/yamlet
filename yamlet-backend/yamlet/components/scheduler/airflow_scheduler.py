# my_etl_project/components/scheduler/airflow_scheduler.py
import logging
import time
import requests
from .base_scheduler import BaseScheduler

logger = logging.getLogger(__name__)

class AirflowScheduler(BaseScheduler):
    """
    Scheduler that triggers an Airflow DAG run via the Airflow REST API.
    
    In this example, when scheduling is triggered, it makes an HTTP POST request
    to the Airflow API to create a new DAG run for the given pipeline.
    
    Make sure your Airflow instance is running and the API is accessible.
    """
    def __init__(self, dag_id: str):
        """
        Initialize with the Airflow DAG ID to trigger.
        
        Args:
            dag_id (str): The ID of the DAG to trigger in Airflow.
        """
        self.dag_id = dag_id
        self.airflow_url = "http://localhost:8080/api/v1/dags"  # Adjust URL as needed

    def schedule(self, pipeline_id: str, cron_expr: str = None):
        """
        Triggers an Airflow DAG run for the specified pipeline.
        
        Args:
            pipeline_id (str): Unique identifier for the pipeline.
            cron_expr (str, optional): Cron expression (can be ignored if using Airflow scheduling).
        """
        # Create a unique dag_run_id using the pipeline_id and current timestamp.
        dag_run_id = f"{pipeline_id}_run_{int(time.time())}"
        payload = {
            "dag_run_id": dag_run_id,
            "conf": {"pipeline_id": pipeline_id}
        }
        url = f"{self.airflow_url}/{self.dag_id}/dagRuns"
        logger.info("Triggering Airflow DAG '%s' for pipeline '%s'.", self.dag_id, pipeline_id)
        try:
            response = requests.post(url, json=payload)
            if response.status_code in (200, 201):
                logger.info("Airflow DAG triggered successfully: %s", response.json())
            else:
                logger.error("Failed to trigger Airflow DAG: %s", response.text)
                raise Exception(f"Airflow scheduling failed: {response.text}")
        except Exception as e:
            logger.error("Error triggering Airflow DAG: %s", e)
            raise
