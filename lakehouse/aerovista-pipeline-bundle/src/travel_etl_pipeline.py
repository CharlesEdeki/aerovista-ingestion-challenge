# Databricks notebook source
# MAGIC %md
# MAGIC # AeroVista Travel ETL Pipeline
# MAGIC 
# MAGIC This Spark Declarative Pipeline transforms travel trip data from Bronze to Silver layer.
# MAGIC 
# MAGIC **Transformations:**
# MAGIC - Extract date and time components from pickup/dropoff timestamps
# MAGIC - Filter invalid records (null coordinates, negative amounts)
# MAGIC - Reorder columns for better usability

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# Get configuration from pipeline settings
catalog = spark.conf.get("catalog", "aerovista_bootcamp")
bronze_schema = spark.conf.get("bronze_schema", "bronze")
silver_schema = spark.conf.get("silver_schema", "silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Source Data

# COMMAND ----------

@dlt.table(
    name="bronze_travel_trips",
    comment="Raw travel trip data from source system"
)
def bronze_travel_trips():
    """
    Read raw travel trip data from bronze layer.
    """
    return spark.table(f"{catalog}.{bronze_schema}.travel_trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Cleaned and Transformed Data

# COMMAND ----------

@dlt.table(
    name="silver_travel_trips",
    comment="Cleaned and transformed travel trip data with date/time parsing",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_drop({
    "valid_pickup_coordinates": "pickup_longitude IS NOT NULL AND pickup_latitude IS NOT NULL",
    "valid_dropoff_coordinates": "dropoff_longitude IS NOT NULL AND dropoff_latitude IS NOT NULL",
    "valid_fare": "fare_amount >= 0",
    "valid_total": "total_amount >= 0"
})
def silver_travel_trips():
    """
    Transform bronze travel trips data:
    - Extract date and time components
    - Filter invalid records
    - Reorder columns
    """
    df = dlt.read("bronze_travel_trips")
    
    # Extract date and time components using withColumns for better performance
    df_transformed = df.withColumns({
        "date": F.to_date(F.col("tpep_pickup_datetime")),
        "pickup_time": F.date_format(F.col("tpep_pickup_datetime"), "HH:mm:ss"),
        "dropoff_date": F.to_date(F.col("tpep_dropoff_datetime")),
        "dropoff_time": F.date_format(F.col("tpep_dropoff_datetime"), "HH:mm:ss")
    })
    
    # Drop original timestamp columns
    df_transformed = df_transformed.drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
    
    # Reorder columns - date/time columns first
    # Compute columns once to avoid repeated RPC calls
    all_columns = df_transformed.columns
    date_time_cols = ["date", "pickup_time", "dropoff_date", "dropoff_time"]
    remaining_cols = [col for col in all_columns if col not in date_time_cols]
    
    df_final = df_transformed.select(date_time_cols + remaining_cols)
    
    return df_final
