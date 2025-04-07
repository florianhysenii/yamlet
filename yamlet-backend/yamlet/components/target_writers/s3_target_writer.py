from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from .base_target_writer import BaseTargetWriter

class S3TargetWriter(BaseTargetWriter):
    """
    Writes data to Amazon S3 as files in CSV or Parquet format.
    
    Configuration should include:
        - path: The S3 URI (e.g., s3a://bucket/path)
        - format: Either 'csv' or 'parquet'
        - mode: Write mode (append, overwrite, etc.)
        - options: (Optional) Additional write options as a dict.
    """
    def __init__(self, config: Dict):
        super().__init__(config)
        if "path" not in self.config:
            raise ValueError("S3 target config must include 'path'.")
        if "format" not in self.config:
            raise ValueError("S3 target config must include 'format' ('csv' or 'parquet').")
        self.path = self.config["path"]
        self.format = self.config["format"].lower()
        if self.format not in ("csv", "parquet"):
            raise ValueError("S3 target 'format' must be either 'csv' or 'parquet'.")
        self.mode = self.config.get("mode", "append").lower()
        self.options = self.config.get("options", {})

    def write(self, spark: SparkSession, df: DataFrame):
        """
        Writes the DataFrame to S3 using the specified file format and options.
        
        Args:
            spark (SparkSession): The active Spark session.
            df (DataFrame): The DataFrame to write.
        """
        writer = df.write.format(self.format).mode(self.mode)
        if isinstance(self.options, dict) and self.options:
            writer = writer.options(**self.options)
        writer.save(self.path)
