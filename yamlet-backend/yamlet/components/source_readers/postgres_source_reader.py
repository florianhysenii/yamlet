from pyspark.sql import SparkSession, DataFrame
from .jdbc_source_reader import JDBCSourceReader

class PostgresSourceReader(JDBCSourceReader):
    """Source reader for PostgreSQL databases using JDBC.
    
    Inherits from JDBCSourceReader and provides the PostgreSQL-specific JDBC URL and driver.
    """
    def __init__(self, source_config: dict):
        """
        Initialize the PostgreSQL source reader and set up Postgres-specific JDBC parameters.
        
        Args:
            source_config (dict): Configuration including host, port, database, user, password, table.
        """
        super().__init__(source_config)
        self.jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        self.jdbc_driver = "org.postgresql.Driver"
