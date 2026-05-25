from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from analytics import build_time_series_query, flatten_time_series_rows
from dotenv import load_dotenv
from pymongo import MongoClient
from spark_common import infer_direction, parse_iso_date


def _clean_connection_string(value: str | None) -> str | None:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith('"') and not s.startswith('"'):
        s = s[:-1].rstrip()
    return s or None


def _mongo_uri_from_env(env_file: Path) -> str:
    load_dotenv(env_file)
    mongo_uri = _clean_connection_string(
        os.getenv("MONGO_URI") or os.getenv("MONGODB_ATLAS_URI")
    )
    if not mongo_uri:
        raise RuntimeError("Missing MONGO_URI (or MONGODB_ATLAS_URI) environment variable.")
    return mongo_uri


def _db_name_from_env(env_file: Path) -> str:
    load_dotenv(env_file)
    return (os.getenv("MONGO_DB_NAME") or os.getenv("DB_NAME") or "acme_financial_dw").strip()


def export_time_series_jsonl(
    output_path: Path,
    *,
    asset_id: str | None = None,
    data_source_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    env_file = Path(__file__).resolve().parents[1] / ".env"
    mongo_uri = _mongo_uri_from_env(env_file)
    db_name = _db_name_from_env(env_file)
    start = parse_iso_date(start_date, "startDate")
    end = parse_iso_date(end_date, "endDate")
    if start and end and start > end:
        raise ValueError("startDate must be on or before endDate.")

    query: dict[str, Any] = {}
    if asset_id and data_source_id:
        query = build_time_series_query(asset_id, data_source_id, start, end)
    elif any((asset_id, data_source_id, start, end)):
        if not asset_id:
            raise ValueError("assetId is required when filtering export data.")
        if not data_source_id:
            raise ValueError("dataSourceId is required when filtering export data.")

    client = MongoClient(mongo_uri, appname="acme-financial-dw-spark-export")
    try:
        docs = list(client[db_name]["time_series"].find(query).sort("timestamp", 1))
    finally:
        client.close()

    rows = flatten_time_series_rows(docs)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            payload = {
                "assetId": row.get("assetId"),
                "dataSourceId": row.get("dataSourceId"),
                "timestamp": row.get("timestamp"),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "volume": row.get("volume"),
            }
            handle.write(json.dumps(payload, sort_keys=True) + "\n")

    return {"rowsExported": len(rows), "output": str(output_path), "filters": query}


def run_spark_aggregate(input_path: Path, output_path: Path | None = None) -> list[dict[str, Any]]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    try:
        spark = SparkSession.builder.appName("acme-dwh-spark-aggregate").master("local[*]").getOrCreate()
    except Exception as exc:
        raise RuntimeError(
            "Failed to start SparkSession. Ensure Java Runtime is installed and available to PySpark."
        ) from exc
    try:
        df = spark.read.json(str(input_path))
        normalized = (
            df.select(
                F.col("assetId").cast("string").alias("assetId"),
                F.col("dataSourceId").cast("string").alias("dataSourceId"),
                F.col("timestamp").cast("string").alias("timestamp"),
                F.col("open").cast("double").alias("open"),
                F.col("high").cast("double").alias("high"),
                F.col("low").cast("double").alias("low"),
                F.col("close").cast("double").alias("close"),
                F.col("volume").cast("double").alias("volume"),
            )
            .where(F.col("assetId").isNotNull() & F.col("dataSourceId").isNotNull() & F.col("timestamp").isNotNull())
        )
        agg = normalized.groupBy("assetId", "dataSourceId").agg(
            F.count("*").alias("count"),
            F.min("timestamp").alias("startTimestamp"),
            F.max("timestamp").alias("endTimestamp"),
            F.min("close").alias("closeMin"),
            F.max("close").alias("closeMax"),
            F.avg("close").alias("closeAvg"),
            F.min("open").alias("openMin"),
            F.max("open").alias("openMax"),
            F.avg("open").alias("openAvg"),
            F.min("high").alias("highMin"),
            F.max("high").alias("highMax"),
            F.avg("high").alias("highAvg"),
            F.min("low").alias("lowMin"),
            F.max("low").alias("lowMax"),
            F.avg("low").alias("lowAvg"),
            F.min("volume").alias("volumeMin"),
            F.max("volume").alias("volumeMax"),
            F.avg("volume").alias("volumeAvg"),
        )
        ordered = agg.orderBy("assetId", "dataSourceId")
        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            ordered.coalesce(1).write.mode("overwrite").json(str(output_path))
        return [row.asDict(recursive=True) for row in ordered.collect()]
    finally:
        spark.stop()


def run_spark_forecast(
    input_path: Path,
    *,
    asset_id: str,
    data_source_id: str,
) -> dict[str, Any]:
    try:
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import LinearRegression
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Missing Spark ML dependencies. Install required packages (for example: pyspark and numpy)."
        ) from exc
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    try:
        spark = SparkSession.builder.appName("acme-dwh-spark-forecast").master("local[*]").getOrCreate()
    except Exception as exc:
        raise RuntimeError(
            "Failed to start SparkSession. Ensure Java Runtime is installed and available to PySpark."
        ) from exc
    try:
        df = spark.read.json(str(input_path))
        filtered = (
            df.select(
                F.col("assetId").cast("string").alias("assetId"),
                F.col("dataSourceId").cast("string").alias("dataSourceId"),
                F.col("timestamp").cast("string").alias("timestamp"),
                F.col("close").cast("double").alias("close"),
            )
            .where(
                (F.col("assetId") == asset_id)
                & (F.col("dataSourceId") == data_source_id)
                & F.col("timestamp").isNotNull()
                & F.col("close").isNotNull()
            )
            .orderBy("timestamp")
        )

        row_count = filtered.count()
        if row_count < 3:
            raise ValueError(
                "At least 3 valid close rows are required for Spark ML workflow forecasting."
            )

        indexed = filtered.withColumn(
            "timeIndex",
            (F.row_number().over(Window.orderBy("timestamp")) - F.lit(1)).cast("double"),
        )
        assembler = VectorAssembler(inputCols=["timeIndex"], outputCol="features")
        train = assembler.transform(indexed).select(
            "features",
            F.col("close").alias("label"),
            "timeIndex",
            "timestamp",
            "close",
        )
        model = LinearRegression(featuresCol="features", labelCol="label").fit(train)
        metrics = model.summary

        latest = train.orderBy(F.desc("timestamp")).first()
        latest_close = float(latest["close"])
        max_index = float(train.agg(F.max("timeIndex").alias("maxIndex")).first()["maxIndex"])

        next_row = spark.createDataFrame([(max_index + 1.0,)], ["timeIndex"])
        predicted_next = float(
            model.transform(assembler.transform(next_row)).select("prediction").first()["prediction"]
        )
        direction = infer_direction(latest_close, predicted_next)

        return {
            "assetId": asset_id,
            "dataSourceId": data_source_id,
            "model": "Spark MLlib LinearRegression",
            "trainingRows": row_count,
            "features": ["timeIndex"],
            "label": "close",
            "coefficients": [float(value) for value in model.coefficients],
            "intercept": float(model.intercept),
            "rmse": float(metrics.rootMeanSquaredError),
            "r2": float(metrics.r2),
            "latestTimestamp": latest["timestamp"],
            "latestClose": latest_close,
            "predictedNextClose": predicted_next,
            "direction": direction,
            "note": "Educational ML workflow output from historical data only. Not financial advice.",
        }
    finally:
        spark.stop()


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Unsupported JSON value: {type(value)!r}")


def _print_json(payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=_json_default))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Spark aggregation and Spark ML forecasting utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    export_parser = subparsers.add_parser("export", help="Export MongoDB time_series rows to JSONL.")
    export_parser.add_argument("--output", required=True, help="Output JSONL path.")
    export_parser.add_argument("--asset-id", default=None, help="Optional assetId filter.")
    export_parser.add_argument("--data-source-id", default=None, help="Optional dataSourceId filter.")
    export_parser.add_argument("--start-date", default=None, help="Optional start date (YYYY-MM-DD).")
    export_parser.add_argument("--end-date", default=None, help="Optional end date (YYYY-MM-DD).")

    aggregate_parser = subparsers.add_parser("aggregate", help="Run Spark aggregation on flattened JSONL.")
    aggregate_parser.add_argument("--input", required=True, help="Input JSON or JSONL path.")
    aggregate_parser.add_argument("--output", default=None, help="Optional output directory for Spark JSON files.")

    forecast_parser = subparsers.add_parser("forecast", help="Run Spark MLlib forecast for one asset/source pair.")
    forecast_parser.add_argument("--input", required=True, help="Input JSON or JSONL path.")
    forecast_parser.add_argument("--asset-id", required=True, help="assetId to model.")
    forecast_parser.add_argument("--data-source-id", required=True, help="dataSourceId to model.")
    forecast_parser.add_argument("--output", default=None, help="Optional output JSON file path.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "export":
            result = export_time_series_jsonl(
                Path(args.output),
                asset_id=args.asset_id,
                data_source_id=args.data_source_id,
                start_date=args.start_date,
                end_date=args.end_date,
            )
            _print_json(result)
            return 0

        if args.command == "aggregate":
            rows = run_spark_aggregate(Path(args.input), Path(args.output) if args.output else None)
            _print_json(rows)
            return 0

        if args.command == "forecast":
            forecast = run_spark_forecast(
                Path(args.input),
                asset_id=args.asset_id,
                data_source_id=args.data_source_id,
            )
            if args.output:
                output_path = Path(args.output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text(json.dumps(forecast, indent=2, sort_keys=True), encoding="utf-8")
            _print_json(forecast)
            return 0
    except (RuntimeError, ValueError) as exc:
        _print_json({"status": "error", "message": str(exc)})
        return 1

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
