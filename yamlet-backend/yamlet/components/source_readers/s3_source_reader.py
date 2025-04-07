from pyspark.sql import SparkSession, DataFrame
from .base_source_reader import BaseSourceReader

class S3SourceReader(BaseSourceReader):
    """Source reader for Amazon S3, supporting CSV and Parquet file formats.
    
    Reads data from files in S3 using Spark's DataFrame reader (not via JDBC).
    """
    def __init__(self, source_config: dict):
        """
        Initialize the S3 source reader with configuration for S3 file input.
        
        Args:
            source_config (dict): Configuration for S3 input with keys:
                - path: S3 URI (e.g., "s3a://bucket/path/to/file").
                - format: Data format, either "csv" or "parquet".
                - options: (Optional) dict of additional read options (e.g., header, inferSchema for CSV).
        """
        super().__init__(source_config)
        if not self.config.get('path'):
            raise ValueError("S3 source config missing 'path' to the file(s).")
        if not self.config.get('format'):
            raise ValueError("S3 source config missing 'format' (expected 'csv' or 'parquet').")
        fmt = self.config['format'].lower()
        if fmt not in ('csv', 'parquet'):
            raise ValueError("S3 source 'format' must be 'csv' or 'parquet'.")
        self.path = self.config['path']
        self.format = fmt
        opts = self.config.get('options', {}) or {}
        if not isinstance(opts, dict):
            raise ValueError("S3 source 'options' must be a dictionary if provided.")
        self.options = opts
    
    def read(self, spark: SparkSession) -> DataFrame:
        """
        Read the S3 data using Spark and return a DataFrame.
        
        Uses Spark's DataFrameReader with the specified format and options to load the file(s).
        Note: Ensure AWS credentials or Hadoop configuration for S3 are set in Spark if needed.
        
        Args:
            spark (SparkSession): The Spark session to use for reading.
        Returns:
            DataFrame: The resulting DataFrame loaded from the S3 source.
        """
        reader = spark.read.format(self.format)
        if self.options:
            reader = reader.options(**self.options)
        return reader.load(self.path)
