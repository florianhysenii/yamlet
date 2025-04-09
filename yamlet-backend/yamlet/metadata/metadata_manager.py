# my_etl_project/metadata/metadata_manager.py
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class PipelineMetadataManager:
    """
    Manages pipeline metadata by saving pipeline configurations (YAML)
    along with versioning information into a MySQL metadata database.
    
    The corresponding table (pipeline_metadata) can be created with the following SQL:
    
    CREATE TABLE IF NOT EXISTS pipeline_metadata (
        id INT AUTO_INCREMENT PRIMARY KEY,
        pipeline_name VARCHAR(255) NOT NULL,
        version INT NOT NULL,
        yaml_config TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
    """
    def __init__(self, host: str, user: str, password: str, database: str):
        try:
            self.connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            if self.connection.is_connected():
                self.cursor = self.connection.cursor(dictionary=True)
                self._create_table_if_not_exists()
        except Error as e:
            raise Exception(f"Error connecting to MySQL: {e}")

    def _create_table_if_not_exists(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS pipeline_metadata (
                id INT AUTO_INCREMENT PRIMARY KEY,
                pipeline_name VARCHAR(255) NOT NULL,
                version INT NOT NULL,
                yaml_config TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            );
        """
        self.cursor.execute(create_table_query)
        self.connection.commit()
        logger.info("Metadata table ensured.")

    def save_pipeline(self, pipeline_name: str, yaml_config: str) -> int:
        """
        Saves the pipeline YAML configuration into the metadata database.
        Only saves a new version if the YAML has changed compared to the latest record.
        
        Args:
            pipeline_name (str): The name of the pipeline.
            yaml_config (str): The YAML configuration of the pipeline.
        
        Returns:
            int: The current version number of the pipeline.
        """
        try:
            # Retrieve the latest version record for this pipeline
            query = """
                SELECT version, yaml_config FROM pipeline_metadata 
                WHERE pipeline_name = %s 
                ORDER BY version DESC LIMIT 1
            """
            self.cursor.execute(query, (pipeline_name,))
            result = self.cursor.fetchone()
            logger.info("Fetched existing metadata: %s", result)

            # If there's an existing record, check if the YAML is the same.
            if result and result.get("yaml_config", "").strip() == yaml_config.strip():
                logger.info("No changes detected in pipeline '%s'. Keeping version %d.", pipeline_name, result["version"])
                return result["version"]

            new_version = result["version"] + 1 if result else 1

            insert_query = """
                INSERT INTO pipeline_metadata (pipeline_name, version, yaml_config, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
            """
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.cursor.execute(insert_query, (pipeline_name, new_version, yaml_config, now, now))
            self.connection.commit()
            logger.info("Saved pipeline '%s' as version %d.", pipeline_name, new_version)
            return new_version
        except Error as e:
            self.connection.rollback()
            logger.error("Error saving pipeline metadata: %s", e)
            raise Exception(f"Error saving pipeline metadata: {e}")

    def get_pipeline_config(self, pipeline_name: str, version: int = None):
        """
        Retrieves the YAML configuration and metadata for a given pipeline.
        
        Args:
            pipeline_name (str): The name of the pipeline.
            version (int, optional): Specific version to retrieve. If omitted, returns the latest version.
            
        Returns:
            dict: A dictionary containing the metadata record (or None if not found).
        """
        try:
            if version is None:
                query = """
                    SELECT * FROM pipeline_metadata 
                    WHERE pipeline_name = %s 
                    ORDER BY version DESC LIMIT 1
                """
                self.cursor.execute(query, (pipeline_name,))
            else:
                query = """
                    SELECT * FROM pipeline_metadata 
                    WHERE pipeline_name = %s AND version = %s
                """
                self.cursor.execute(query, (pipeline_name, version))
            result = self.cursor.fetchone()
            logger.info("Fetched pipeline config: %s", result)
            return result
        except Error as e:
            logger.error("Error retrieving pipeline metadata: %s", e)
            raise Exception(f"Error retrieving pipeline metadata: {e}")

    def get_pipeline_versions(self, pipeline_name: str):
        """
        Retrieves all versions for the specified pipeline.
        
        Args:
            pipeline_name (str): The pipeline name.
            
        Returns:
            list: A list of metadata records.
        """
        try:
            query = """
                SELECT * FROM pipeline_metadata 
                WHERE pipeline_name = %s
                ORDER BY version DESC
            """
            self.cursor.execute(query, (pipeline_name,))
            result = self.cursor.fetchall()
            logger.info("Fetched pipeline versions: %s", result)
            return result
        except Error as e:
            logger.error("Error retrieving pipeline versions: %s", e)
            raise Exception(f"Error retrieving pipeline versions: {e}")

    def close(self):
        if self.connection.is_connected():
            self.cursor.close()
            self.connection.close()