import logging
from typing import Callable
from pyspark.sql import DataFrame
from .base_transformation import BaseTransformation

logger = logging.getLogger(__name__)

class PythonTransformation(BaseTransformation):
    """
    Allows a custom Python callable to transform the DataFrame.
    
    This is intended for small, self-contained transformations.
    In production environments, using SQL transformations may be preferred
    due to easier auditing and sandboxing.
    """
    def __init__(self, transformation_func: Callable[[DataFrame], DataFrame]):
        """
        Initializes the transformation with a callable function.
        
        Args:
            transformation_func (Callable[[DataFrame], DataFrame]): 
                A function that takes a DataFrame and returns a transformed DataFrame.
        """
        self.transformation_func = transformation_func

    def apply(self, df: DataFrame) -> DataFrame:
        logger.info("Applying Python transformation.")
        try:
            return self.transformation_func(df)
        except Exception as e:
            logger.error(f"Error during Python transformation: {e}")
            raise
