import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, avg, min, max, stddev, to_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
import os

# --- Schema Definitions ---
WAFER_LOG_SCHEMA = StructType([
    StructField("wafer_id", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("process_flow", ArrayType(StructType([
        StructField("step_id", IntegerType(), True),
        StructField("step_name", StringType(), True),
        StructField("equipment_id", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration_sec", IntegerType(), True)
    ])), True)
])

# --- Main Spark Job ---
def process_semiconductor_data(spark, execution_date, raw_bucket, processed_bucket, use_s3=False):
    print(f"Starting Spark transformation for date: {execution_date}")

    date_str_nodash = execution_date.replace('-', '')

    # --- Paths ---
    if use_s3:
        sensor_path = f"s3a://{raw_bucket}/transactional/sensor/dt={execution_date}/sensor_readings_{date_str_nodash}.csv"
        yield_path = f"s3a://{raw_bucket}/transactional/yield/dt={execution_date}/yield_results_{date_str_nodash}.csv"
        wafer_path = f"s3a://{raw_bucket}/transactional/wafer/dt={execution_date}/wafer_logs_{date_str_nodash}.json"
        processed_s3_base_path = f"s3a://{processed_bucket}"
    else:
        sensor_path = os.path.join(raw_bucket, f"sensor_readings_{date_str_nodash}.csv")
        yield_path = os.path.join(raw_bucket, f"yield_results_{date_str_nodash}.csv")
        wafer_path = os.path.join(raw_bucket, f"wafer_logs_{date_str_nodash}.json")
        processed_s3_base_path = processed_bucket

    # --- 1. Load Raw Data ---
    sensor_df = spark.read.csv(sensor_path, header=True, inferSchema=True).withColumn("timestamp", to_timestamp(col("timestamp")))
    yield_df = spark.read.csv(yield_path, header=True, inferSchema=True)
    wafer_logs_df = spark.read.json(wafer_path, schema=WAFER_LOG_SCHEMA)

    # --- 2. Transform Data ---
    fact_wafer_yield = yield_df.select(
        col("wafer_id"),
        col("batch_id"),
        col("final_yield_percentage"),
        col("failure_reason_code"),
        lit(execution_date).alias("dt")
    )

    process_steps_df = wafer_logs_df.select(
        col("wafer_id"),
        explode("process_flow").alias("step")
    ).select(
        col("wafer_id"),
        col("step.step_id"),
        col("step.step_name"),
        col("step.equipment_id"),
        col("step.start_time"),
        col("step.end_time"),
        col("step.duration_sec")
    )

    sensor_agg_df = sensor_df.join(
        process_steps_df,
        (sensor_df.wafer_id == process_steps_df.wafer_id) &
        (sensor_df.equipment_id == process_steps_df.equipment_id) &
        (sensor_df.timestamp >= process_steps_df.start_time) &
        (sensor_df.timestamp <= process_steps_df.end_time),
        'inner'
    ).groupBy(
        process_steps_df.wafer_id,
        process_steps_df.step_id,
        process_steps_df.equipment_id,
        sensor_df.sensor_name
    ).agg(
        avg("sensor_value").alias("avg_value"),
        min("sensor_value").alias("min_value"),
        max("sensor_value").alias("max_value"),
        stddev("sensor_value").alias("stddev_value")
    )

    pivoted_sensors_df = sensor_agg_df.groupBy("wafer_id", "step_id", "equipment_id") \
        .pivot("sensor_name") \
        .agg(
            avg("avg_value").alias("avg"),
            max("max_value").alias("max"),
            stddev("stddev_value").alias("stddev")
        )

    fact_process_measurements = process_steps_df.join(
        pivoted_sensors_df,
        ["wafer_id", "step_id", "equipment_id"],
        "left"
    ).select(
        process_steps_df.wafer_id,
        process_steps_df.step_id,
        process_steps_df.equipment_id,
        process_steps_df.duration_sec,
        lit(execution_date).alias("dt"),
        *[c for c in pivoted_sensors_df.columns if c not in ["wafer_id", "step_id", "equipment_id"]]
    )

    # --- 3. Write Transformed Data ---
    print("Writing fact_wafer_yield...")
    fact_wafer_yield.write.mode("overwrite").partitionBy("dt").parquet(f"{processed_s3_base_path}/fact_wafer_yield/")

    print("Writing fact_process_measurements...")
    fact_process_measurements.write.mode("overwrite").partitionBy("dt").parquet(f"{processed_s3_base_path}/fact_process_measurements/")

    print("Spark transformation job complete.")


# --- Entry Point ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_date", required=True, help="DAG execution date in YYYY-MM-DD")
    parser.add_argument("--raw_bucket", required=True, help="Raw data path (local folder or S3 bucket)")
    parser.add_argument("--processed_bucket", required=True, help="Processed data path (local folder or S3 bucket)")
    parser.add_argument("--use_s3", action="store_true", help="Set this flag to use S3 paths")
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"Semiconductor_ETL_{args.execution_date}").getOrCreate()
    process_semiconductor_data(spark, args.execution_date, args.raw_bucket, args.processed_bucket, use_s3=args.use_s3)
    spark.stop()
