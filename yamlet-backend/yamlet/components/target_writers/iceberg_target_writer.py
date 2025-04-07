from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from .base_target_writer import BaseTargetWriter

class IcebergTargetWriter(BaseTargetWriter):
    """
    Writes data to an Apache Iceberg table.
    
    Supports various modes:
     - "append": Append data to the table.
     - "overwrite": Overwrite the entire table.
     - "merge": Perform a MERGE operation to upsert records.
    
    For MERGE, it expects additional configuration such as 'merge_keys' to
    define the join keys.
    """
    def __init__(self, config: Dict):
        super().__init__(config)
        if "path" not in self.config:
            raise ValueError("Iceberg target config must include 'path'.")
        self.table_path = self.config["path"]
        self.mode = self.config.get("mode", "append").lower()
        self.merge_keys = self.config.get("merge_keys", [])

    def write(self, spark: SparkSession, df: DataFrame):
        """
        Writes a DataFrame to an Iceberg table using the configured mode.
        
        For mode 'merge', a MERGE statement is constructed using the merge_keys.
        
        Args:
            spark (SparkSession): The active Spark session.
            df (DataFrame): The DataFrame to write.
        """
        if self.mode == "append":
            (df.write.format("iceberg")
             .mode("append")
             .save(self.table_path))
        elif self.mode == "overwrite":
            (df.write.format("iceberg")
             .mode("overwrite")
             .save(self.table_path))
        elif self.mode == "merge":
            temp_view = "temp_merge_view"
            df.createOrReplaceTempView(temp_view)

            if not self.merge_keys:
                raise ValueError("Merge mode requires 'merge_keys' in configuration.")

            merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in self.merge_keys])
            merge_sql = f"""
                MERGE INTO {self.table_path} t
                USING {temp_view} s
                ON {merge_condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """
            spark.sql(merge_sql)
        else:
            raise ValueError(f"Unsupported write mode: {self.mode}")
