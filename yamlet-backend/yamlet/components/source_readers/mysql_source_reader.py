from pyspark.sql import SparkSession, DataFrame
from .jdbc_source_reader import JDBCSourceReader

class MySQLSourceReader(JDBCSourceReader):
    """Source reader for MySQL databases using JDBC.
    
    Inherits from JDBCSourceReader and provides the MySQL-specific JDBC URL and driver.
    """
    def __init__(self, source_config: dict):
        """
        Initialize the MySQL source reader and set up MySQL-specific JDBC parameters.
        
        Args:
            source_config (dict): Configuration including host, port, database, user, password, table.
        """
        super().__init__(source_config)
        self.jdbc_url = f"jdbc:mysql://{self.host}:{self.port}/{self.database}"
        self.jdbc_driver = "com.mysql.cj.jdbc.Driver"
