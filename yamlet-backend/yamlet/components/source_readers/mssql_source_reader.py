from pyspark.sql import SparkSession, DataFrame
from .jdbc_source_reader import JDBCSourceReader

class MSSQLSourceReader(JDBCSourceReader):
    """Source reader for Microsoft SQL Server databases using JDBC.
    
    Inherits from JDBCSourceReader and provides the MSSQL-specific JDBC URL and driver.
    """
    def __init__(self, source_config: dict):
        """
        Initialize the MSSQL source reader and set up SQL Server-specific JDBC parameters.
        
        Args:
            source_config (dict): Configuration including host, port, database, user, password, table.
        """
        super().__init__(source_config)
        self.jdbc_url = f"jdbc:sqlserver://{self.host}:{self.port};databaseName={self.database}"
        self.jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
