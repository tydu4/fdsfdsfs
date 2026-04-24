from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

from .common import ensure_dir, find_source_file, maybe_write_parquet_from_csv, write_csv_rows
from .constants import RAW_DIRNAME, SOURCES
from .settings import OUTPUT_DIR, SOURCE_DIR
from .xlsx_xml import max_columns, read_first_sheet_rows


def _xlsx_rows_for_raw(source_file: Path, col_count: int):
    row_num = 1
    for row in read_first_sheet_rows(source_file):
        payload = {
            "source_file": source_file.name,
            "source_row_num": row_num,
        }
        for i in range(col_count):
            payload[f"col_{i + 1}"] = row[i] if i < len(row) else ""
        row_num += 1
        yield payload


def _csv_rows_for_raw(source_file: Path, col_count: int):
    with source_file.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh, delimiter=";")
        for row_num, row in enumerate(reader, start=1):
            payload = {
                "source_file": source_file.name,
                "source_row_num": row_num,
            }
            for i in range(col_count):
                payload[f"col_{i + 1}"] = row[i] if i < len(row) else ""
            yield payload


def _detect_csv_width(source_file: Path) -> int:
    width = 0
    with source_file.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh, delimiter=";")
        for row in reader:
            if len(row) > width:
                width = len(row)
    return width


def run(input_dir: Path, out_dir: Path) -> dict[str, object]:
    raw_dir = out_dir / RAW_DIRNAME
    ensure_dir(raw_dir)

    metrics: dict[str, object] = {
        "stage": "prepare_raw",
        "input_dir": str(input_dir),
        "raw_dir": str(raw_dir),
        "sources": [],
    }

    for src in SOURCES:
        source_file = find_source_file(input_dir, src.prefix, src.kind)
        if src.kind == "xlsx":
            col_count = max_columns(source_file)
            row_iter = _xlsx_rows_for_raw(source_file, col_count)
        else:
            col_count = _detect_csv_width(source_file)
            row_iter = _csv_rows_for_raw(source_file, col_count)

        fieldnames = ["source_file", "source_row_num"] + [f"col_{i + 1}" for i in range(col_count)]
        raw_csv_path = raw_dir / src.raw_output_name
        row_count = write_csv_rows(raw_csv_path, fieldnames, row_iter)

        parquet_path = raw_csv_path.with_suffix(".parquet")
        parquet_ok, parquet_msg = maybe_write_parquet_from_csv(raw_csv_path, parquet_path)
        if not parquet_ok and parquet_path.exists():
            parquet_path.unlink(missing_ok=True)

        metrics["sources"].append(
            {
                "source_prefix": src.prefix.strip(),
                "source_file": source_file.name,
                "raw_output": raw_csv_path.name,
                "rows_written": row_count,
                "columns_written": col_count,
                "parquet": parquet_msg,
            }
        )

    metrics_path = raw_dir / "load_metrics_raw.json"
    with metrics_path.open("w", encoding="utf-8") as fh:
        json.dump(metrics, fh, ensure_ascii=False, indent=2)

    return metrics


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    metrics = run(Path(SOURCE_DIR), Path(OUTPUT_DIR))
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
