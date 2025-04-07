from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from typing import Dict

class BaseTargetWriter(ABC):
    """
    Abstract base class defining the contract for any target writer.
    """
    def __init__(self, config: Dict):
        self.config = config

    @abstractmethod
    def write(self, spark: SparkSession, df: DataFrame):
        """
        Writes the Spark DataFrame to the designated target.
        
        Args:
            spark (SparkSession): The active Spark session.
            df (DataFrame): The DataFrame to write.
        """
        pass
