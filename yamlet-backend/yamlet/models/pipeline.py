from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from yamlet.components.source_readers.base_source_reader import BaseSourceReader
from yamlet.components.target_writers.base_target_writer import BaseTargetWriter
from yamlet.components.transformations.base_transformation import BaseTransformation
from yamlet.components.scheduler.base_scheduler import BaseScheduler

class Pipeline:
    """
    A pipeline object representing the ETL flow:
      1. Read from source
      2. Apply transformations
      3. Write to target
      4. (Optionally) schedule
    """

    def __init__(
        self,
        name: str,
        source_reader: BaseSourceReader,
        transformations: List[BaseTransformation],
        target_writer: BaseTargetWriter,
        scheduler: BaseScheduler = None,
        schedule_cron: str = None,
    ):
        self.name = name
        self.source_reader = source_reader
        self.transformations = transformations
        self.target_writer = target_writer
        self.scheduler = scheduler
        self.schedule_cron = schedule_cron  # e.g. "0 2 * * *"

    def run(self, spark: SparkSession) -> None:
        """
        Orchestrates the pipeline: read -> transform -> write.
        """
        df_source = self.source_reader.read(spark)

        current_df = df_source
        for transform in self.transformations:
            current_df = transform.apply(current_df)

        self.target_writer.write(spark, current_df)

    def schedule_pipeline(self, pipeline_id: str):
        """
        If a scheduler is configured, schedule the pipeline with the provided cron expression.
        """
        if self.scheduler and self.schedule_cron:
            self.scheduler.schedule(pipeline_id, self.schedule_cron)
