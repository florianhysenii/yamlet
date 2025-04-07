from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, when

class SCD2Handler:
    """
    Handles Slowly Changing Dimensions Type 2 (SCD2) merges.
    
    SCD2 enables maintaining historical records by closing out old versions
    and inserting new records when changes occur.
    
    Attributes:
        config (Dict[str, Any]): SCD2 configuration. Expected keys include:
            - "enabled" (bool): Whether SCD2 logic is applied.
            - "key_columns" (List[str]): Columns that uniquely identify a record.
            - "effective_date_column" (str): Column for the record's start validity.
            - "end_date_column" (str): Column for the record's end validity.
            - "current_flag_column" (str): Column that flags the current version.
            - "version_column" (str): Column that tracks the record version.
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes an SCD2Handler with a configuration dictionary.
        
        Args:
            config (Dict[str, Any]): The SCD2 configuration.
        """
        self.config = config or {}
        self.enabled = self.config.get("enabled", False)
        self.key_columns = self.config.get("key_columns", [])
        self.effective_date_column = self.config.get("effective_date_column", "valid_from")
        self.end_date_column = self.config.get("end_date_column", "valid_to")
        self.current_flag_column = self.config.get("current_flag_column", "is_current")
        self.version_column = self.config.get("version_column", "version")

    def apply_scd2(self, target_df: DataFrame, source_df: DataFrame) -> DataFrame:
        """
        Applies SCD2 logic to merge new (source) data into the existing (target)
        dimension table. It performs the following steps:
          1. Augment the source DataFrame with SCD2 columns.
          2. Join source and target on key columns.
          3. Determine updates by comparing source and target.
          4. Close out outdated records by setting an end date and marking them inactive.
          5. Insert new/updated records with an incremented version.
        
        Args:
            target_df (DataFrame): The existing dimension table.
            source_df (DataFrame): New or changed records.
        
        Returns:
            DataFrame: The merged dimension table with SCD2 logic applied.
        """
        if not self.enabled:
            return source_df

        source_aug = (source_df
                      .withColumn(self.effective_date_column, current_timestamp())
                      .withColumn(self.version_column, lit(1))
                      .withColumn(self.current_flag_column, lit(True)))

        join_condition = self._build_merge_condition(target_df, source_aug)
        joined_df = target_df.alias("t").join(source_aug.alias("s"), join_condition, "fullouter")
        
        merged_df = joined_df.select(*self._get_merge_columns())

        merged_df = merged_df.withColumn(
            self.end_date_column,
            when(col(self.current_flag_column) == False, current_timestamp()).otherwise(None)
        )
        return merged_df

    def _build_merge_condition(self, target_df: DataFrame, source_df: DataFrame):
        """
        Builds a join condition expression based on key columns.
        
        Args:
            target_df (DataFrame): The target DataFrame alias "t".
            source_df (DataFrame): The source DataFrame alias "s".
        
        Returns:
            Column: A Spark SQL Column representing the join condition.
        """
        from functools import reduce
        import operator
        conditions = [target_df[k] == source_df[k] for k in self.key_columns]
        return reduce(operator.and_, conditions)

    def _get_merge_columns(self) -> List[Any]:
        """
        Constructs the list of column expressions to be selected after the join,
        choosing values from source or target as appropriate.
        
        Returns:
            List[Any]: List of Spark Column expressions for the merged DataFrame.
        """
        from pyspark.sql.functions import when, col
        columns = []
        for key in self.key_columns:
            columns.append(
                when(col(f"s.{key}").isNotNull(), col(f"s.{key}"))
                .otherwise(col(f"t.{key}"))
                .alias(key)
            )
        scd_columns = [self.effective_date_column, self.end_date_column, self.current_flag_column, self.version_column]
        columns.extend([
            when(col(f"s.{self.effective_date_column}").isNotNull(), 
                 col(f"s.{self.effective_date_column}"))
            .otherwise(col(f"t.{self.effective_date_column}"))
            .alias(self.effective_date_column),
            when(col(f"s.{self.version_column}").isNotNull(), 
                 col(f"s.{self.version_column}"))
            .otherwise(col(f"t.{self.version_column}"))
            .alias(self.version_column),
            when(col(f"s.{self.current_flag_column}").isNotNull(), 
                 col(f"s.{self.current_flag_column}"))
            .otherwise(col(f"t.{self.current_flag_column}"))
            .alias(self.current_flag_column)
        ])
        return columns
