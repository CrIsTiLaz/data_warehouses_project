from __future__ import annotations

from pathlib import Path

import src.spark_analytics as spark_analytics


def test_build_parser_supports_aggregate_command() -> None:
    parser = spark_analytics.build_parser()
    args = parser.parse_args(["aggregate", "--input", "data/in.jsonl", "--output", "data/out"])

    assert args.command == "aggregate"
    assert args.input == "data/in.jsonl"
    assert args.output == "data/out"


def test_main_aggregate_calls_runner_and_returns_zero(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_spark_aggregate(input_path: Path, output_path: Path | None = None):
        captured["input"] = input_path
        captured["output"] = output_path
        return [{"assetId": "TSLA", "count": 3}]

    def fake_print_json(payload):
        captured["payload"] = payload

    monkeypatch.setattr(spark_analytics, "run_spark_aggregate", fake_run_spark_aggregate)
    monkeypatch.setattr(spark_analytics, "_print_json", fake_print_json)

    exit_code = spark_analytics.main(
        ["aggregate", "--input", "data/time_series_export.sample.jsonl", "--output", "data/spark_out"]
    )

    assert exit_code == 0
    assert captured["input"] == Path("data/time_series_export.sample.jsonl")
    assert captured["output"] == Path("data/spark_out")
    assert captured["payload"] == [{"assetId": "TSLA", "count": 3}]


def test_main_forecast_value_error_returns_one(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_spark_forecast(input_path: Path, *, asset_id: str, data_source_id: str):
        raise ValueError("At least 3 valid close rows are required for Spark ML workflow forecasting.")

    def fake_print_json(payload):
        captured["payload"] = payload

    monkeypatch.setattr(spark_analytics, "run_spark_forecast", fake_run_spark_forecast)
    monkeypatch.setattr(spark_analytics, "_print_json", fake_print_json)

    exit_code = spark_analytics.main(
        [
            "forecast",
            "--input",
            "data/time_series_export.sample.jsonl",
            "--asset-id",
            "TSLA",
            "--data-source-id",
            "alpha_vantage_api_v1",
        ]
    )

    assert exit_code == 1
    assert captured["payload"] == {
        "status": "error",
        "message": "At least 3 valid close rows are required for Spark ML workflow forecasting.",
    }
