from __future__ import annotations

import csv
import os
import re
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Iterator, Sequence


_LS_NUMERIC_RE = re.compile(r"^\d+(?:\.0+)?$")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def find_source_file(input_dir: Path, prefix: str, expected_kind: str) -> Path:
    candidates = sorted(
        p
        for p in input_dir.iterdir()
        if p.is_file()
        and p.name.startswith(prefix)
        and ((expected_kind == "xlsx" and p.suffix.lower() == ".xlsx") or (expected_kind == "csv" and p.suffix.lower() == ".csv"))
    )
    if not candidates:
        raise FileNotFoundError(f"Source file not found for prefix '{prefix}' in {input_dir}")
    return candidates[0]


def list_raw_files(input_dir: Path) -> list[Path]:
    return sorted(p for p in input_dir.iterdir() if p.is_file() and p.suffix.lower() == ".csv")


def normalize_ls_id(raw: str) -> str | None:
    value = (raw or "").strip()
    if not value:
        return None
    if not _LS_NUMERIC_RE.match(value):
        return None
    if "." in value:
        value = value.split(".", 1)[0]
    return value


def parse_date_ddmmyyyy(raw: str) -> date | None:
    value = (raw or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%d.%m.%Y").date()
    except ValueError:
        return None


def excel_serial_to_date(raw: str) -> date | None:
    value = (raw or "").strip()
    if not value:
        return None
    try:
        serial = int(float(value))
    except ValueError:
        return None
    base = datetime(1899, 12, 30)
    return (base + timedelta(days=serial)).date()


def parse_date_any(raw: str) -> date | None:
    value = (raw or "").strip()
    if value:
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass

    dt = parse_date_ddmmyyyy(raw)
    if dt is not None:
        return dt
    return excel_serial_to_date(raw)


def parse_decimal(raw: str) -> Decimal | None:
    value = (raw or "").strip()
    if not value:
        return None
    value = value.replace(" ", "").replace(",", ".")
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def parse_yes_no(raw: str) -> int | None:
    value = (raw or "").strip().lower()
    if value in {"да", "yes", "y", "1", "true"}:
        return 1
    if value in {"нет", "no", "n", "0", "false"}:
        return 0
    if value == "":
        return None
    return None


def is_valid_uuid(raw: str) -> bool:
    value = (raw or "").strip()
    if not value:
        return False
    try:
        uuid.UUID(value)
        return True
    except (ValueError, TypeError):
        return False


def add_months(d: date, months: int) -> date:
    year = d.year + (d.month - 1 + months) // 12
    month = (d.month - 1 + months) % 12 + 1
    return date(year, month, 1)


def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def parse_as_of_month(raw: str) -> date:
    value = (raw or "").strip()
    try:
        return datetime.strptime(value, "%Y-%m").date().replace(day=1)
    except ValueError as exc:
        raise ValueError("as-of-month must be in format YYYY-MM") from exc


def decimal_to_str(value: Decimal | None) -> str:
    if value is None:
        return ""
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def date_to_str(value: date | None) -> str:
    return value.isoformat() if value else ""


def write_csv_rows(path: Path, fieldnames: Sequence[str], rows: Iterable[dict[str, object]]) -> int:
    ensure_dir(path.parent)
    count = 0
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: "" if v is None else v for k, v in row.items()})
            count += 1
    return count


def read_csv_dict(path: Path) -> Iterator[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield {k: (v if v is not None else "") for k, v in row.items()}


def maybe_write_parquet_from_csv(csv_path: Path, parquet_path: Path) -> tuple[bool, str]:
    try:
        import pyarrow.csv as pacsv  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except ImportError:
        return (False, "pyarrow_not_installed")

    try:
        table = pacsv.read_csv(str(csv_path))
        pq.write_table(table, str(parquet_path))
        return (True, "ok")
    except Exception as exc:  # pragma: no cover - runtime-specific
        return (False, f"error:{exc}")


def csv_row_count(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return max(sum(1 for _ in fh) - 1, 0)


def discover_input_layer(input_dir: Path, expected_subdir: str) -> Path:
    direct = input_dir
    nested = input_dir / expected_subdir
    if nested.exists() and nested.is_dir():
        return nested
    return direct
