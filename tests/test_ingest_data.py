from unittest.mock import MagicMock
from src.ingest_data import get_env, load_config, build_stream, start_write

from unittest.mock import MagicMock, patch
from src import ingest_data

def test_main_covers_runtime_flow(monkeypatch):
    # Ensure config is stable
    monkeypatch.setenv("INPUT_PATH", "dbfs:/input/")
    monkeypatch.setenv("SCHEMA_LOCATION", "dbfs:/schema/")
    monkeypatch.setenv("CHECKPOINT_LOCATION", "dbfs:/checkpoint/")
    monkeypatch.setenv("TARGET_TABLE", "main.default.raw_ingested_data")

    # Fake SparkSession class
    fake_spark_session = MagicMock()
    spark = MagicMock()
    fake_spark_session.builder.appName.return_value.getOrCreate.return_value = spark

    df = MagicMock()
    query = MagicMock()

    with patch("src.ingest_data.build_stream", return_value=df) as mock_build, \
         patch("src.ingest_data.start_write", return_value=query) as mock_write:
        ingest_data.main(spark_session_cls=fake_spark_session)

    fake_spark_session.builder.appName.assert_called_once_with("IngestionJob")
    mock_build.assert_called_once()     # build_stream called
    mock_write.assert_called_once()     # start_write called
    query.awaitTermination.assert_called_once()


def test_get_env_default_when_missing(monkeypatch):
    monkeypatch.delenv("FOO", raising=False)
    assert get_env("FOO", "bar") == "bar"


def test_load_config_uses_defaults(monkeypatch):
    # Clear env vars so defaults execute (covers config lines)
    for k in ["INPUT_PATH", "SCHEMA_LOCATION", "CHECKPOINT_LOCATION", "TARGET_TABLE"]:
        monkeypatch.delenv(k, raising=False)

    cfg = load_config()

    assert cfg["input_path"]
    assert cfg["schema_location"]
    assert cfg["checkpoint_location"]
    assert cfg["target_table"]


def test_build_stream_and_start_write_execute_builder_lines():
    # ---- build_stream() ----
    spark = MagicMock()

    # spark.readStream.format(...).option(...).load(...)
    fmt = spark.readStream.format.return_value
    fmt.option.return_value = fmt  # chain option()
    fmt.load.return_value = "DF"

    cfg = {
        "input_path": "dbfs:/input/",
        "schema_location": "dbfs:/schema/",
        "checkpoint_location": "dbfs:/chk/",
        "target_table": "main.default.raw_ingested_data",
    }

    df = build_stream(spark, cfg)
    assert df == "DF"

    spark.readStream.format.assert_called_once_with("cloudFiles")
    fmt.option.assert_any_call("cloudFiles.format", "csv")
    fmt.option.assert_any_call("cloudFiles.schemaLocation", cfg["schema_location"])
    fmt.load.assert_called_once_with(cfg["input_path"])

    # ---- start_write() ----
    df_obj = MagicMock()
    w = df_obj.writeStream.format.return_value
    w.option.return_value = w         # chain option()
    w.trigger.return_value = w        # chain trigger()
    w.toTable.return_value = "QUERY"

    q = start_write(df_obj, cfg)
    assert q == "QUERY"

    df_obj.writeStream.format.assert_called_once_with("delta")
    w.option.assert_called_once_with("checkpointLocation", cfg["checkpoint_location"])
    w.trigger.assert_called_once_with(availableNow=True)
    w.toTable.assert_called_once_with(cfg["target_table"])