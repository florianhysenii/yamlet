import logging
from crontab import CronTab
from .base_scheduler import BaseScheduler

# Configure a module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# If needed, attach handlers (e.g., StreamHandler) in your main application setup

class CronScheduler(BaseScheduler):
    """
    Uses the local user's crontab to schedule pipeline runs.
    
    This implementation leverages the python-crontab package to programmatically
    manage crontab entries. It removes any existing job for the given pipeline_id
    before adding the new schedule.
    """
    
    def schedule(self, pipeline_id: str, cron_expr: str = None):
        """
        Schedules the pipeline using the provided cron expression.
        
        Args:
            pipeline_id (str): Unique pipeline identifier.
            cron_expr (str, optional): Cron expression for the schedule.
            
        Raises:
            ValueError: If no cron expression is provided.
        """
        if not cron_expr:
            logger.warning("No cron expression provided; skipping scheduling.")
            return
        
        command = f"python /path/to/yamlet/main.py run --pipeline_id {pipeline_id}"
        
        cron = CronTab(user=True)
        
        cron.remove_all(comment=f"pipeline_{pipeline_id}")
        
        # Create a new job with the specified command and cron schedule.
        job = cron.new(command=command, comment=f"pipeline_{pipeline_id}")
        try:
            job.setall(cron_expr)
        except Exception as e:
            logger.error(f"Failed to set cron expression '{cron_expr}' for pipeline {pipeline_id}: {e}")
            raise e
        
        cron.write()
        logger.info(f"Scheduled pipeline {pipeline_id} with cron expression: {cron_expr}")
