from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from .base_target_writer import BaseTargetWriter

class JDBCTargetWriter(BaseTargetWriter):
    """
    Writes data to a relational database (e.g., MySQL, PostgreSQL, MSSQL) via JDBC.
    
    Expects configuration parameters similar to the JDBC source:
        - host, port, database (or db), table, user, password.
    Additionally, a 'mode' can be provided (e.g., append, overwrite).
    """
    def __init__(self, config: Dict):
        super().__init__(config)
        required_keys = ["host", "port", "database", "table", "user", "password"]
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"JDBC target config missing required key: '{key}'")
        self.host = self.config["host"]
        self.port = self.config["port"]
        self.database = self.config["database"]
        self.table = self.config["table"]
        self.user = self.config["user"]
        self.password = self.config["password"]
        self.mode = self.config.get("mode", "append").lower()
        self.db_type = self.config.get("db_type", "mysql").lower()  # default to MySQL if not provided
        self.jdbc_url, self.jdbc_driver = self._build_jdbc_details()

    def _build_jdbc_details(self):
        if self.db_type == "mysql":
            url = f"jdbc:mysql://{self.host}:{self.port}/{self.database}"
            driver = "com.mysql.cj.jdbc.Driver"
        elif self.db_type == "postgresql":
            url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
            driver = "org.postgresql.Driver"
        elif self.db_type == "mssql":
            url = f"jdbc:sqlserver://{self.host}:{self.port};databaseName={self.database}"
            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        else:
            raise ValueError(f"Unsupported db_type: {self.db_type}")
        return url, driver

    def write(self, spark: SparkSession, df: DataFrame):
        """
        Writes a DataFrame to a JDBC target using the specified configuration.
        
        Args:
            spark (SparkSession): The active Spark session.
            df (DataFrame): The DataFrame to write.
        """
        (df.write.format("jdbc")
         .option("url", self.jdbc_url)
         .option("driver", self.jdbc_driver)
         .option("dbtable", self.table)
         .option("user", self.user)
         .option("password", self.password)
         .mode(self.mode)
         .save())
