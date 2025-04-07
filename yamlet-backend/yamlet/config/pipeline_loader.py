import yaml
import os
from typing import Dict, Any
from yamlet.models.pipeline import Pipeline

# Import your reader, writer, and transformation classes
from yamlet.components.source_readers.mysql_source_reader import MySQLSourceReader
from yamlet.components.source_readers.postgres_source_reader import PostgresSourceReader
from yamlet.components.source_readers.mssql_source_reader import MSSQLSourceReader
from yamlet.components.source_readers.s3_source_reader import S3SourceReader

from yamlet.components.transformations.sql_transformation import SQLTransformation
from yamlet.components.transformations.python_transformation import PythonTransformation

from yamlet.components.target_writers.iceberg_target_writer import IcebergTargetWriter
from yamlet.components.target_writers.jdbc_target_writer import JDBCTargetWriter
from yamlet.components.target_writers.s3_target_writer import S3TargetWriter

from yamlet.components.scheduler.cron_scheduler import CronScheduler

# Mapping dictionaries
SOURCE_READERS: Dict[str, Any] = {
    "mysql": MySQLSourceReader,
    "postgresql": PostgresSourceReader,
    "mssql": MSSQLSourceReader,
    "s3": S3SourceReader,
}

TARGET_WRITERS: Dict[str, Any] = {
    "iceberg": IcebergTargetWriter,
    "jdbc": JDBCTargetWriter,
    "s3": S3TargetWriter,
}

TRANSFORMATION_TYPES: Dict[str, Any] = {
    "sql": SQLTransformation,
    "python": PythonTransformation,
}

def load_connections(connections_path: str = "pipelines/connections.yaml") -> Dict[str, Any]:
    """
    Loads connection details from a YAML file.
    
    Args:
        connections_path (str): Path to the connections YAML file.
    
    Returns:
        Dict[str, Any]: A dictionary containing connection details.
    """
    if not os.path.exists(connections_path):
        return {}
    with open(connections_path, "r", encoding="utf-8") as file:
        connections = yaml.safe_load(file)
    return connections.get("connections", {})

def merge_connection_config(base_config: Dict[str, Any], connection_details: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merges connection details into the base configuration.
    
    Args:
        base_config (Dict[str, Any]): The base configuration from the pipeline YAML.
        connection_details (Dict[str, Any]): Connection details loaded from connections.yaml.
    
    Returns:
        Dict[str, Any]: The merged configuration.
    """
    merged_config = dict(base_config)
    merged_config.update(connection_details)
    return merged_config

def load_pipeline_config(file_path: str) -> Pipeline:
    """
    Loads a pipeline configuration from a YAML file, instantiates the correct 
    source reader, transformations, target writer, and scheduler, and returns a Pipeline object.

    Args:
        file_path (str): The path to the pipeline YAML configuration.

    Returns:
        Pipeline: A pipeline object containing the components to run.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Pipeline config not found at: {file_path}")

    with open(file_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    name = config.get("name", "UnnamedPipeline")

    connections = load_connections()

    # 1) Source
    source_section = config.get("source", {})
    source_type = source_section.get("type", "").lower()
    source_config = source_section.get("config", {})
    connection_name = source_section.get("connection")
    if connection_name:
        conn_details = connections.get("mysql", {}).get(connection_name, {})
        source_config = merge_connection_config(source_config, conn_details)
    if source_type not in SOURCE_READERS:
        raise ValueError(f"Unsupported or missing source type: {source_type}")
    source_reader = SOURCE_READERS[source_type](source_config)

    # 2) Transformations
    transformations = []
    for t in config.get("transformations", []):
        t_type = t.get("type", "").lower()
        t_conf = t.get("config", {})
        if t_type not in TRANSFORMATION_TYPES:
            raise ValueError(f"Unknown transformation type: {t_type}")
        if t_type == "sql":
            sql_query = t_conf.get("query")
            if not sql_query:
                raise ValueError("SQL transformation requires a 'query' parameter.")
            transformations.append(TRANSFORMATION_TYPES[t_type](sql_query))
        elif t_type == "python":
            def dummy_python_transform(df):
                return df.filter("some_column IS NOT NULL")
            transformations.append(TRANSFORMATION_TYPES[t_type](dummy_python_transform))

    # 3) Target
    target_section = config.get("target", {})
    target_type = target_section.get("type", "").lower()
    target_config = target_section.get("config", {})
    connection_name = target_section.get("connection")
    if connection_name:
        conn_details = connections.get("mysql", {}).get(connection_name, {})
        target_config = merge_connection_config(target_config, conn_details)
    if target_type not in TARGET_WRITERS:
        raise ValueError(f"Unsupported or missing target type: {target_type}")
    target_writer = TARGET_WRITERS[target_type](target_config)

    # 4) Scheduler
    schedule_cron = config.get("schedule_cron", "")
    scheduler_type = config.get("scheduler", "").lower()
    scheduler_instance = CronScheduler() if scheduler_type == "cron" else None

    pipeline = Pipeline(
        name=name,
        source_reader=source_reader,
        transformations=transformations,
        target_writer=target_writer,
        scheduler=scheduler_instance,
        schedule_cron=schedule_cron,
    )

    return pipeline
