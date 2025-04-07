from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseTransformation(ABC):
    """
    Abstract base class for transformations.
    """

    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame:
        """
        Applies a transformation to the input DataFrame and returns a new DataFrame.
        
        Args:
            df (DataFrame): Input Spark DataFrame.
        
        Returns:
            DataFrame: Transformed Spark DataFrame.
        """
        pass
