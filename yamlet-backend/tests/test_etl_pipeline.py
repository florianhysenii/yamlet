import logging
import pytest
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
)
from yamlet.components.logic.cdc_handler import CDCHandler
from yamlet.components.logic.scd2_handler import SCD2Handler
from yamlet.components.transformations.python_transformation import PythonTransformation

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing (local mode)."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("ETLTest") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_full_etl_pipeline(spark):
    """Test the full ETL pipeline end-to-end with dummy data, including CDC, transformations, and SCD2 logic."""
    logger.info("Setting up dummy source data for MySQL and PostgreSQL.")

    data_mysql = [
        (1, "Alice", 100, "2024-06-01 00:00:00"),   # New record (should pass CDC)
        (2, "Bob",   200, "2023-06-01 00:00:00")      # Old record (should be filtered out)
    ]
    data_postgres = [
        (3, "Charlie", 300, "2024-07-01 00:00:00"),   # New record (should pass CDC)
        (4, "Diana",   400, "2023-05-01 00:00:00")      # Old record (should be filtered out)
    ]
    source_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("score", IntegerType(), False),
        StructField("update_ts", TimestampType(), False)
    ])
    df_mysql = spark.createDataFrame(data_mysql, schema=source_schema)
    df_postgres = spark.createDataFrame(data_postgres, schema=source_schema)

    logger.info("Applying CDC filtering based on 'update_ts'.")
    # 2. Apply CDC Filtering:
    # For testing, we create a CDC configuration that uses a cutoff of "2024-01-01 00:00:00".
    # In a real scenario, CDCHandler._get_last_sync_time() should return this value.
    cdc_config = {
        "enabled": True,
        "mode": "timestamp",
        "timestamp": {"column": "update_ts"}
    }
    # For testing purposes, monkey-patch _get_last_sync_time to return our cutoff.
    cdc_handler = CDCHandler(cdc_config)
    cdc_handler._get_last_sync_time = lambda: "2024-01-01 00:00:00"

    filtered_mysql_df = cdc_handler.apply_cdc(df_mysql)
    filtered_postgres_df = cdc_handler.apply_cdc(df_postgres)
    increment_df = filtered_mysql_df.union(filtered_postgres_df)

    # Verify CDC filtering: only records with id 1 and 3 should remain.
    remaining_ids = {row["id"] for row in increment_df.collect()}
    assert remaining_ids == {1, 3}, f"Expected ids {{1, 3}}, got {remaining_ids}"
    assert increment_df.filter(F.col("id") == 2).count() == 0, "Old record id 2 should be filtered out."
    assert increment_df.filter(F.col("id") == 4).count() == 0, "Old record id 4 should be filtered out."
    logger.info("CDC filtering verified successfully.")

    # 3. Apply Transformations:
    # Define a simple Python transformation that uppercases 'name' and creates a 'score_category'.
    def transform_func(df):
        df = df.withColumn("name", F.upper(F.col("name")))
        df = df.withColumn("score_category", F.when(F.col("score") > 250, "High").otherwise("Low"))
        return df

    python_transformation = PythonTransformation(transform_func)
    transformed_df = python_transformation.apply(increment_df)

    # Verify that transformation does not lose records and applies changes.
    assert transformed_df.count() == increment_df.count(), "Record count should remain unchanged after transformation."
    names = [row["name"] for row in transformed_df.collect()]
    assert all(name.isupper() for name in names), "All names should be uppercased after transformation."
    if "score_category" in transformed_df.columns:
        row_id1 = transformed_df.filter(F.col("id") == 1).collect()[0]
        row_id3 = transformed_df.filter(F.col("id") == 3).collect()[0]
        assert row_id1["score_category"] == "Low", f"Expected score_category 'Low' for id 1, got {row_id1['score_category']}"
        assert row_id3["score_category"] == "High", f"Expected score_category 'High' for id 3, got {row_id3['score_category']}"
    logger.info("Transformation applied and verified successfully.")

    # 4. Setup initial target (SCD2 dimension) DataFrame.
    logger.info("Setting up initial target (SCD2) dimension data.")
    target_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("score", IntegerType(), False),
        StructField("score_category", StringType(), False),
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), True),
        StructField("current_flag", BooleanType(), False),
        StructField("version", IntegerType(), False)
    ])
    initial_target_data = [
        # id,    name,    score, score_category,            valid_from,              valid_to, current_flag, version
        (1, "ALICE", 260, "High", datetime(2023, 1, 1, 0, 0, 0), None, True, 1),  # Existing record for id 1
        (2, "BOB",   200, "Low",  datetime(2023, 1, 1, 0, 0, 0), None, True, 1)   # Existing record for id 2
    ]
    target_df = spark.createDataFrame(initial_target_data, schema=target_schema)

    # 5. Apply SCD2 Merge:
    logger.info("Applying SCD2 merge.")
    scd2_config = {
        "enabled": True,
        "key_columns": ["id"],
        "effective_date_column": "valid_from",
        "end_date_column": "valid_to",
        "current_flag_column": "current_flag",
        "version_column": "version"
    }
    scd2_handler = SCD2Handler(scd2_config)
    result_df = scd2_handler.apply_scd2(target_df, transformed_df)

    # 6. Verify SCD2 outputs:
    # Expected:
    # - id=1: two records (old expired and new current, with version increment)
    # - id=2: unchanged (one record current)
    # - id=3: new record inserted (one record current)
    assert result_df.count() == 4, f"Expected 4 records after SCD2 merge, got {result_df.count()}"
    distinct_ids = {row["id"] for row in result_df.select("id").distinct().collect()}
    assert distinct_ids == {1, 2, 3}, f"Expected ids {{1, 2, 3}}, got {distinct_ids}"
    assert result_df.filter((F.col("id") == 1) & (F.col("current_flag") == True)).count() == 1, "Id 1 should have one current record."
    assert result_df.filter((F.col("id") == 1) & (F.col("current_flag") == False)).count() == 1, "Id 1 should have one expired record."
    assert result_df.filter((F.col("id") == 2) & (F.col("current_flag") == True)).count() == 1, "Id 2 should have one current record."
    assert result_df.filter((F.col("id") == 2) & (F.col("current_flag") == False)).count() == 0, "Id 2 should have no expired record."
    assert result_df.filter((F.col("id") == 3) & (F.col("current_flag") == True)).count() == 1, "Id 3 should have one current record."
    assert result_df.filter((F.col("id") == 3) & (F.col("current_flag") == False)).count() == 0, "Id 3 should have no expired record."

    id1_records = result_df.filter(F.col("id") == 1).collect()
    id1_old = next((row for row in id1_records if not row["current_flag"]), None)
    id1_new = next((row for row in id1_records if row["current_flag"]), None)
    assert id1_old is not None and id1_new is not None, "Id 1 should have both old and new records."
    assert id1_new["version"] == id1_old["version"] + 1, "New version for id 1 should be old version + 1."
    assert id1_old["valid_to"] == id1_new["valid_from"], "Old record valid_to should match new record valid_from for id 1."
    assert id1_new["valid_to"] is None, "New record valid_to should be None for id 1."
    # Field values:
    # - id=1: initial record had score=260 ("High"), new source record has score=100 ("Low")
    assert id1_old["score"] == 260 and id1_new["score"] == 100, "Score for id 1 did not update correctly."
    assert id1_old["score_category"] == "High" and id1_new["score_category"] == "Low", "Score category for id 1 did not update correctly."

    id2_record = result_df.filter(F.col("id") == 2).collect()[0]
    assert id2_record["current_flag"] == True and id2_record["valid_to"] is None, "Id 2 should remain current."
    assert id2_record["version"] == 1, "Id 2 version should remain 1."
    assert id2_record["name"] == "BOB" and id2_record["score"] == 200, "Id 2 field values should remain unchanged."

    id3_record = result_df.filter(F.col("id") == 3).collect()[0]
    assert id3_record["current_flag"] == True and id3_record["valid_to"] is None, "Id 3 should be current."
    assert id3_record["version"] == 1, "Id 3 should have version 1."
    assert id3_record["name"] == "CHARLIE" and id3_record["score"] == 300, "Id 3 field values should match the source data."

    logger.info("Full ETL pipeline test passed.")

if __name__ == "__main__":
    spark_session = SparkSession.builder.master("local[1]").appName("ETLTest").getOrCreate()
    try:
        test_full_etl_pipeline(spark_session)
        logger.info("ETL pipeline test executed successfully.")
    finally:
        spark_session.stop()
