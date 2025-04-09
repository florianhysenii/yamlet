import os
import logging
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
import sys

from yamlet.components.logic.cdc_handler import CDCHandler
from yamlet.components.logic.scd2_handler import SCD2Handler

# Import new source reader classes
from yamlet.components.source_readers.mysql_source_reader import MySQLSourceReader
from yamlet.components.source_readers.postgres_source_reader import PostgresSourceReader
from yamlet.components.source_readers.mssql_source_reader import MSSQLSourceReader
from yamlet.components.source_readers.s3_source_reader import S3SourceReader

# Import new target writer classes
from yamlet.components.target_writers.iceberg_target_writer import IcebergTargetWriter
from yamlet.components.target_writers.jdbc_target_writer import JDBCTargetWriter
from yamlet.components.target_writers.s3_target_writer import S3TargetWriter

# Set up module-level logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Handlers (e.g., StreamHandler) should be configured in the main application if not already.

# Mappings for dynamic instantiation
SOURCE_READERS = {
    "mysql": MySQLSourceReader,
    "postgresql": PostgresSourceReader,
    "mssql": MSSQLSourceReader,
    "s3": S3SourceReader,
}

TARGET_WRITERS = {
    "iceberg": IcebergTargetWriter,
    "jdbc": JDBCTargetWriter,
    "s3": S3TargetWriter,
}

class SparkEngine:
    """
    A Spark engine that executes ETL pipelines with CDC and SCD2 support.
    
    It supports multiple source types (MySQL, PostgreSQL, MSSQL, S3) and target types
    (Iceberg, JDBC, S3) by delegating reading and writing to pluggable components.
    """
    def __init__(self, app_name: str = "MyETLApp", master: str = "local[*]"):
        self.app_name = app_name
        self.master = master
        self.spark = None
        # CDC and SCD2 handlers
        self.cdc_handler = None
        self.scd2_handler = None

    def start(self):
        """
        Start or get a local Spark session, adding necessary JARs to the classpath.
        """
        if not self.spark:
            jar_dir = os.path.abspath("yamlet/jars")
            mysql_jar = os.path.join(jar_dir, "mysql-connector-java-8.0.31.jar")
            postgres_jar = os.path.join(jar_dir, "postgresql-42.5.0.jar")
            iceberg_jar = os.path.join(jar_dir, "iceberg-spark-runtime-3.3_2.12-1.2.1.jar")
            mssql_jar = os.path.join(jar_dir, "sqljdbc42.jar")  # adjust as necessary

            # Use semicolon on Windows, colon otherwise.
            sep = ";" if sys.platform.startswith("win") else ":"
            extra_classpath = sep.join([mysql_jar, postgres_jar, mssql_jar, iceberg_jar])
            
            logger.info("Starting Spark session with extra classpath: %s", extra_classpath)
            self.spark = (
                SparkSession.builder
                .appName(self.app_name)
                .master(self.master)
                .config("spark.driver.extraClassPath", extra_classpath)
                .config("spark.executor.extraClassPath", extra_classpath)
                .getOrCreate()
            )

    def stop(self):
        """
        Stop the Spark session if it is running.
        """
        if self.spark:
            self.spark.stop()
            self.spark = None
            logger.info("Spark session stopped.")

    def configure_pipeline(self, pipeline_config: Dict[str, Any]):
        """
        Configure CDC and SCD2 handlers based on pipeline configuration.
        """
        source_config = pipeline_config.get("source", {}).get("config", {})
        target_config = pipeline_config.get("target", {}).get("config", {})

        cdc_config = source_config.get("cdc", {})
        if cdc_config.get("enabled", False):
            self.cdc_handler = CDCHandler(cdc_config)
            logger.info("CDC enabled with mode: %s", cdc_config.get("mode", "timestamp"))

        scd2_config = target_config.get("scd2", {})
        if scd2_config.get("enabled", False):
            self.scd2_handler = SCD2Handler(scd2_config)
            logger.info("SCD2 enabled with key columns: %s", scd2_config.get("key_columns", []))

    def execute_pipeline(self, pipeline_config: Dict[str, Any]):
        """
        Execute the ETL pipeline with optional CDC and SCD2 transformations.
        """
        self.start()
        self.configure_pipeline(pipeline_config)

        source_reader = self._get_source_reader(pipeline_config["source"])
        source_df = source_reader.read(self.spark)
        logger.info("Source DataFrame read successfully.")

        if self.cdc_handler:
            source_df = self.cdc_handler.apply_cdc(source_df)
            logger.info("CDC applied to source DataFrame.")

        target_df = None
        if self.scd2_handler:
            try:
                target_df = self._get_target_reader(pipeline_config["target"]).read(self.spark)
                logger.info("Target DataFrame read successfully.")
            except Exception as e:
                logger.warning("Could not read target data; proceeding without existing target: %s", e)
                target_df = None

        if target_df is not None and self.scd2_handler:
            source_df = self.scd2_handler.apply_scd2(target_df, source_df)
            logger.info("SCD2 merge applied.")

        target_writer = self._get_target_writer(pipeline_config["target"])
        target_writer.write(self.spark, source_df)
        logger.info("Final DataFrame written to target.")

    def _get_source_reader(self, source_config: Dict[str, Any]):
        """
        Instantiates and returns a source reader based on the configuration.
        
        Args:
            source_config (Dict[str, Any]): Source configuration dictionary with keys:
                - type: e.g., "mysql", "postgresql", "mssql", "s3"
                - config: additional source-specific parameters.
        
        Returns:
            An instance of a subclass of BaseSourceReader.
        """
        stype = source_config.get("type")
        cfg = source_config.get("config", {})
        reader_cls = SOURCE_READERS.get(stype.lower())
        if not reader_cls:
            raise ValueError(f"Unsupported source type: {stype}")
        logger.info("Using source reader: %s", reader_cls.__name__)
        return reader_cls(cfg)

    def _get_target_writer(self, target_config: Dict[str, Any]):
        """
        Instantiates and returns a target writer based on the configuration.
        
        Args:
            target_config (Dict[str, Any]): Target configuration dictionary with keys:
                - type: e.g., "iceberg", "jdbc", "s3"
                - config: additional target-specific parameters.
        
        Returns:
            An instance of a subclass of BaseTargetWriter.
        """
        ttype = target_config.get("type")
        cfg = target_config.get("config", {})
        writer_cls = TARGET_WRITERS.get(ttype.lower())
        if not writer_cls:
            raise ValueError(f"Unsupported target type: {ttype}")
        logger.info("Using target writer: %s", writer_cls.__name__)
        return writer_cls(cfg)

    def _get_target_reader(self, target_config: Dict[str, Any]) -> DataFrame:
        """
        Reads existing target data. This method is used when applying SCD2 merges.
        
        For now, it supports reading from Iceberg (or any target that can be read as a DataFrame).
        
        Args:
            target_config (Dict[str, Any]): Target configuration dictionary.
        
        Returns:
            DataFrame: The existing target DataFrame.
        """
        ttype = target_config.get("type")
        cfg = target_config.get("config", {})
        if ttype.lower() == "iceberg":
            return self.spark.read.format("iceberg").load(cfg["path"])
        elif ttype.lower() == "jdbc":
            db_type = cfg.get("db_type", "mysql").lower()
            if db_type == "mysql":
                url = f"jdbc:mysql://{cfg['host']}:{cfg['port']}/{cfg['database']}"
                driver = "com.mysql.cj.jdbc.Driver"
            elif db_type == "postgresql":
                url = f"jdbc:postgresql://{cfg['host']}:{cfg['port']}/{cfg['database']}"
                driver = "org.postgresql.Driver"
            elif db_type == "mssql":
                url = f"jdbc:sqlserver://{cfg['host']}:{cfg['port']};databaseName={cfg['database']}"
                driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
            else:
                raise ValueError(f"Unsupported db_type: {db_type}")
            return (self.spark.read.format("jdbc")
                    .option("url", url)
                    .option("driver", driver)
                    .option("user", cfg["user"])
                    .option("password", cfg["password"])
                    .load())
        elif ttype.lower() == "s3":
            fmt = cfg.get("format", "parquet").lower()
            return self.spark.read.format(fmt).load(cfg["path"])
        else:
            raise ValueError(f"Unsupported target type for reading: {ttype}")
