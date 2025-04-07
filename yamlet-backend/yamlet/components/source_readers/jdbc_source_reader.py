from abc import ABC
from pyspark.sql import SparkSession, DataFrame
from .base_source_reader import BaseSourceReader

class JDBCSourceReader(BaseSourceReader, ABC):
    """Abstract base class for JDBC-based source readers.
    
    Encapsulates common JDBC connection logic such as handling host, port, database,
    user, and password from the configuration. This class should be subclassed by 
    specific database source readers (e.g., MySQLSourceReader).
    """
    def __init__(self, source_config: dict):
        """
        Initialize the JDBC source reader with config and validate common parameters.
        
        Args:
            source_config (dict): Configuration for the JDBC source.
                Expected keys include:
                - host: Database host name or address.
                - port: Database port number.
                - database (or db): Name of the database.
                - user: Username for the database.
                - password: Password for the database.
                - table: Name of the table to read (or use a query).
        """
        super().__init__(source_config)
        db_name = self.config.get('database') or self.config.get('db')
        if not db_name:
            raise ValueError("JDBC source config missing 'database' (or 'db') name.")
        self.database = db_name
        for key in ['host', 'port', 'user', 'password', 'table']:
            if key not in self.config or self.config[key] is None:
                raise ValueError(f"JDBC source config missing required key: '{key}'.")
        self.host = self.config['host']
        self.port = self.config['port']
        self.user = self.config['user']
        self.password = self.config['password']
        self.table = self.config['table']
        self.jdbc_url: str = None
        self.jdbc_driver: str = None
    
    def read(self, spark: SparkSession) -> DataFrame:
        """
        Perform the JDBC read using Spark, returning a DataFrame.
        
        Uses the JDBC URL and driver specified by the subclass, along with common 
        parameters (user, password, table) to load data from the database.
        
        Args:
            spark (SparkSession): The Spark session to use for reading.
        Returns:
            DataFrame: The resulting DataFrame loaded from the JDBC source.
        """
        if not self.jdbc_url or not self.jdbc_driver:
            raise ValueError("JDBC URL and driver must be set in the subclass before reading.")
        return (spark.read.format("jdbc")
                .option("url", self.jdbc_url)
                .option("driver", self.jdbc_driver)
                .option("dbtable", self.table)
                .option("user", self.user)
                .option("password", self.password)
                .load())
