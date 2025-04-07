from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame

class BaseSourceReader(ABC):
    """Base abstract class for all source readers in the ETL pipeline.
    
    All source readers should inherit from this class and implement the
    `read` method which returns a Spark DataFrame.
    """
    def __init__(self, source_config: dict):
        """
        Initialize the source reader with a configuration dictionary.
        
        Args:
            source_config (dict): Configuration parameters for the data source.
        """
        self.config = source_config or {}
    
    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        """
        Read data from the source and return a Spark DataFrame.
        
        Args:
            spark (SparkSession): The Spark session to use for reading the data.
        Returns:
            DataFrame: A Spark DataFrame containing the data from the source.
        """
        raise NotImplementedError("Subclasses must implement the read method.")
