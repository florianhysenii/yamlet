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
                  - query: A SQL query to read data.
        """
        super().__init__(source_config)
        
        # Validate required keys (excluding table)
        db_name = self.config.get('database') or self.config.get('db')
        if not db_name:
            raise ValueError("JDBC source config missing 'database' (or 'db') name.")
        self.database = db_name
        
        for key in ['host', 'port', 'user', 'password']:
            if key not in self.config or self.config[key] is None:
                raise ValueError(f"JDBC source config missing required key: '{key}'.")
        self.host = self.config['host']
        self.port = self.config['port']
        self.user = self.config['user']
        self.password = self.config['password']
        
        # Instead of table, we now require a query.
        self.query = self.config.get('query')
        
        self.jdbc_url: str = None  # to be set in subclass
        self.jdbc_driver: str = None  # to be set in subclass

    def read(self, spark: SparkSession) -> DataFrame:
        """
        Reads data from the JDBC source using Spark.
        Uses the 'query' option exclusively.
        
        Args:
            spark (SparkSession): The Spark session.
        Returns:
            DataFrame: Data loaded using the specified query.
        """
        if not self.jdbc_url or not self.jdbc_driver:
            raise ValueError("JDBC URL and driver must be set in the subclass before reading.")
        if self.query:
            return (spark.read
                    .format("jdbc")
                    .option("url", self.jdbc_url)
                    .option("driver", self.jdbc_driver)
                    .option("query", self.query)
                    .option("user", self.user)
                    .option("password", self.password)
                    .load())
        else:
            raise ValueError("JDBC source config must specify a 'query'.")
