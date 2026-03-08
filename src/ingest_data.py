import os

def get_env(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v if v else default

def load_config() -> dict:
    return {
        "input_path": get_env("INPUT_PATH", "dbfs:/FileStore/ingestion/input/"),
        "schema_location": get_env("SCHEMA_LOCATION", "dbfs:/FileStore/ingestion/schema/"),
        "checkpoint_location": get_env("CHECKPOINT_LOCATION", "dbfs:/FileStore/ingestion/checkpoint/"),
        "target_table": get_env("TARGET_TABLE", "main.default.raw_ingested_data"),
    }

def build_stream(spark, cfg: dict):
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", cfg["schema_location"])
            .option("header", "true")
            .option("inferColumnTypes", "true")
            .load(cfg["input_path"])
    )

def start_write(df, cfg: dict):
    return (
        df.writeStream
          .format("delta")
          .option("checkpointLocation", cfg["checkpoint_location"])
          .trigger(availableNow=True)
          .toTable(cfg["target_table"])
    )

# def main(spark_session_cls=None) -> None:
#     if spark_session_cls is None:
#         from pyspark.sql import SparkSession
#         spark_session_cls = SparkSession

#     cfg = load_config()
#     spark = spark_session_cls.builder.appName("IngestionJob").getOrCreate()
#     df = build_stream(spark, cfg)
#     query = start_write(df, cfg)
#     query.awaitTermination()