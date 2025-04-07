from abc import ABC, abstractmethod

class BaseScheduler(ABC):
    """
    Abstract base class for scheduling the pipeline (daily, hourly, manual, etc.).
    """

    @abstractmethod
    def schedule(self, pipeline_id: str, cron_expr: str = None):
        """
        Schedules the pipeline run according to the desired strategy.
        
        Args:
            pipeline_id (str): Unique identifier for the pipeline.
            cron_expr (str, optional): Cron expression specifying the schedule.
        """
        pass
