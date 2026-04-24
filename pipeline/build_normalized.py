from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Iterator

from .common import (
    add_months,
    date_to_str,
    decimal_to_str,
    discover_input_layer,
    ensure_dir,
    excel_serial_to_date,
    is_valid_uuid,
    maybe_write_parquet_from_csv,
    normalize_ls_id,
    parse_date_any,
    parse_date_ddmmyyyy,
    parse_decimal,
    parse_yes_no,
    read_csv_dict,
    write_csv_rows,
)
from .constants import ACTION_TYPE_BY_PREFIX, NORMALIZED_DIRNAME, RAW_DIRNAME, SOURCES
from .data_dictionary import write_data_dictionary
from .dq import DQCollector
from .settings import OUTPUT_DIR


LS_PROFILE_BINARY_FIELDS: list[tuple[int, str]] = [
    (2, "can_remote_disconnect"),
    (3, "has_phone"),
    (4, "has_benefits"),
    (5, "is_gasified"),
    (6, "is_city"),
    (7, "has_yaroblierc_receipt"),
    (8, "has_post_receipt"),
    (9, "has_e_receipt"),
    (10, "not_resident"),
    (11, "is_chd"),
    (12, "is_mkd"),
    (13, "is_dormitory"),
    (14, "meter_install_tambour"),
    (15, "meter_install_pole"),
    (16, "meter_install_apartment_house"),
    (17, "meter_install_stairwell"),
]

METER_SETUP_FIELDS = {
    "meter_install_tambour",
    "meter_install_pole",
    "meter_install_apartment_house",
    "meter_install_stairwell",
}

ACTION_RAW_FILES = {
    "04_autodial_raw.csv": "autodial",
    "05_email_raw.csv": "email",
    "06_sms_raw.csv": "sms",
    "07_operator_call_raw.csv": "operator_call",
    "08_claim_raw.csv": "claim",
    "09_field_visit_raw.csv": "field_visit",
    "10_restriction_notice_raw.csv": "restriction_notice",
    "11_restriction_raw.csv": "restriction",
    "12_court_order_request_raw.csv": "court_order_request",
    "13_court_order_received_raw.csv": "court_order_received",
}


def _col(row: dict[str, str], idx: int) -> str:
    return (row.get(f"col_{idx}", "") or "").strip()


def _raw_col_count(raw_path: Path) -> int:
    with raw_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
    return len([h for h in header if h.startswith("col_")])


def _collect_raw_by_prefix(raw_dir: Path) -> dict[str, Path]:
    mapping: dict[str, Path] = {}
    for src in SOURCES:
        path = raw_dir / src.raw_output_name
        if path.exists():
            mapping[src.prefix.strip()] = path
    return mapping


def _build_dim_ls_profile(raw_path: Path, dq: DQCollector) -> Iterator[dict[str, str | int]]:
    # aggregator per LS
    agg: dict[str, dict[str, object]] = {}

    for row in read_csv_dict(raw_path):
        source_row_num = row.get("source_row_num", "")
        if source_row_num == "1":
            # header row
            continue

        ls_raw = _col(row, 1)
        ls_id = normalize_ls_id(ls_raw)
        if ls_id is None:
            dq.add(raw_path.name, source_row_num, "NON_NUMERIC_LS", "ЛС не является строкой цифр", ls_raw)
            continue

        item = agg.setdefault(
            ls_id,
            {
                "count": 0,
                "binary_values": {field: set() for _, field in LS_PROFILE_BINARY_FIELDS},
                "row_nums": [],
                "valid_guid_values": set(),
            },
        )
        item["count"] = int(item["count"]) + 1
        item["row_nums"].append(source_row_num)

        binary_values = item["binary_values"]
        for idx, field in LS_PROFILE_BINARY_FIELDS:
            raw_value = _col(row, idx)
            parsed = parse_yes_no(raw_value)
            if parsed is None and raw_value:
                dq.add(raw_path.name, source_row_num, "INVALID_YES_NO", f"Поле {field}: '{raw_value}'", ls_raw)
                continue
            if parsed is not None:
                binary_values[field].add(parsed)

        addr_raw = _col(row, 18)
        if addr_raw:
            if is_valid_uuid(addr_raw):
                item["valid_guid_values"].add(addr_raw)
            else:
                dq.add(raw_path.name, source_row_num, "INVALID_UUID", "Невалидный GUID адреса", ls_raw)

    for ls_id in sorted(agg.keys(), key=lambda x: int(x)):
        item = agg[ls_id]
        binary_values = item["binary_values"]
        out: dict[str, str | int] = {
            "ls_id": ls_id,
            "source_record_count": int(item["count"]),
        }

        for _, field in LS_PROFILE_BINARY_FIELDS:
            values = binary_values[field]
            if not values:
                out[field] = ""
                continue
            if len(values) > 1:
                detail = f"Конфликтующие значения {sorted(values)} для поля {field}; применен max"
                issue_type = "CONFLICT_DUPLICATE_LS_SETUP" if field in METER_SETUP_FIELDS else "CONFLICT_DUPLICATE_LS_BINARY"
                dq.add(raw_path.name, ",".join(item["row_nums"]), issue_type, detail, ls_id)
            out[field] = max(values)

        guid_values = sorted(item["valid_guid_values"])
        if not guid_values:
            out["address_guid"] = ""
        else:
            out["address_guid"] = guid_values[0]
            if len(guid_values) > 1:
                dq.add(
                    raw_path.name,
                    ",".join(item["row_nums"]),
                    "CONFLICT_DUPLICATE_LS_GUID",
                    f"Несколько GUID: {guid_values}",
                    ls_id,
                )

        yield out


def _build_balance_rows(raw_path: Path, dq: DQCollector) -> Iterator[dict[str, str]]:
    col_count = _raw_col_count(raw_path)
    if col_count < 4:
        return

    row_iter = iter(read_csv_dict(raw_path))
    header1 = next(row_iter, None)
    _ = next(row_iter, None)  # second header row

    first_period: date | None = None
    if header1:
        first_period = excel_serial_to_date(_col(header1, 2))
    if first_period is None:
        first_period = date(2025, 1, 1)
        dq.add(raw_path.name, "1", "INVALID_PERIOD_HEADER", "Не удалось прочитать первую дату периода; fallback на 2025-01-01")

    triplets = max((col_count - 1) // 3, 0)
    seen: set[tuple[str, str, str, str, str]] = set()

    for row in row_iter:
        source_row_num = row.get("source_row_num", "")
        ls_raw = _col(row, 1)
        ls_id = normalize_ls_id(ls_raw)
        if ls_id is None:
            dq.add(raw_path.name, source_row_num, "NON_NUMERIC_LS", "ЛС не является строкой цифр", ls_raw)
            continue

        for i in range(triplets):
            period = add_months(first_period, i)
            debt = parse_decimal(_col(row, 2 + 3 * i))
            charge = parse_decimal(_col(row, 3 + 3 * i))
            paid = parse_decimal(_col(row, 4 + 3 * i))

            if debt is None and charge is None and paid is None:
                continue

            debt_s = decimal_to_str(debt)
            charge_s = decimal_to_str(charge)
            paid_s = decimal_to_str(paid)
            key = (ls_id, period.isoformat(), debt_s, charge_s, paid_s)
            if key in seen:
                continue
            seen.add(key)

            yield {
                "ls_id": ls_id,
                "period_month": period.isoformat(),
                "debt_start": debt_s,
                "charge_amt": charge_s,
                "paid_amt": paid_s,
                "source_row_num": source_row_num,
            }


def _build_payment_rows(raw_path: Path, dq: DQCollector) -> Iterator[dict[str, str | int]]:
    for row in read_csv_dict(raw_path):
        source_row_num = row.get("source_row_num", "")
        if source_row_num == "1":
            continue

        ls_raw = _col(row, 1)
        ls_id = normalize_ls_id(ls_raw)
        if ls_id is None:
            dq.add(raw_path.name, source_row_num, "NON_NUMERIC_LS", "ЛС не является строкой цифр", ls_raw)
            continue

        payment_date_raw = _col(row, 2)
        payment_date = parse_date_ddmmyyyy(payment_date_raw)
        if payment_date is None:
            dq.add(raw_path.name, source_row_num, "INVALID_PAYMENT_DATE", f"Невалидная дата оплаты '{payment_date_raw}'", ls_raw)
            continue

        amount_raw = _col(row, 3)
        amount = parse_decimal(amount_raw)
        if amount is None:
            dq.add(raw_path.name, source_row_num, "INVALID_AMOUNT", f"Невалидная сумма '{amount_raw}'", ls_raw)
            continue

        payment_method = _col(row, 4)
        if payment_method == "":
            payment_method = "NULL"

        yield {
            "ls_id": ls_id,
            "payment_date": payment_date.isoformat(),
            "amount_rub": decimal_to_str(amount),
            "payment_method": payment_method,
            "is_reversal": 1 if amount < 0 else 0,
            "source_row_num": source_row_num,
        }


def _build_action_rows(raw_dir: Path, dq: DQCollector) -> Iterator[dict[str, str | int]]:
    counter: Counter[tuple[str, str, str]] = Counter()

    for raw_name, action_type in ACTION_RAW_FILES.items():
        raw_path = raw_dir / raw_name
        if not raw_path.exists():
            continue

        for row in read_csv_dict(raw_path):
            source_row_num = row.get("source_row_num", "")
            if source_row_num in {"1", "2"}:
                continue

            ls_raw = _col(row, 1)
            ls_id = normalize_ls_id(ls_raw)
            if ls_id is None:
                dq.add(raw_path.name, source_row_num, "NON_NUMERIC_LS", "ЛС не является строкой цифр", ls_raw)
                continue

            action_date_raw = _col(row, 2)
            action_date = parse_date_any(action_date_raw)
            if action_date is None:
                dq.add(raw_path.name, source_row_num, "INVALID_ACTION_DATE", f"Невалидная дата '{action_date_raw}'", ls_raw)
                continue

            counter[(ls_id, action_date.isoformat(), action_type)] += 1

    for (ls_id, action_date, action_type), event_count in sorted(counter.items(), key=lambda x: (int(x[0][0]), x[0][1], x[0][2])):
        yield {
            "ls_id": ls_id,
            "action_date": action_date,
            "action_type": action_type,
            "event_count": event_count,
        }


def _parse_limit_value(raw_value: str) -> tuple[str, int | None, int]:
    text = (raw_value or "").strip()
    if not text:
        return text, None, 0
    if "нет огранич" in text.lower():
        return text, None, 1
    cleaned = re.sub(r"[^0-9]", "", text)
    if not cleaned:
        return text, None, 0
    return text, int(cleaned), 0


def _build_action_limits(raw_path: Path, dq: DQCollector) -> Iterator[dict[str, str | int]]:
    for row in read_csv_dict(raw_path):
        source_row_num = row.get("source_row_num", "")
        if source_row_num == "1":
            continue

        measure_name = _col(row, 1)
        raw_limit = _col(row, 2)

        prefix_match = re.match(r"^(\d{2})", measure_name)
        action_type = ""
        if prefix_match:
            action_type = ACTION_TYPE_BY_PREFIX.get(prefix_match.group(1), "") or ""

        if not action_type:
            dq.add(raw_path.name, source_row_num, "UNKNOWN_ACTION_TYPE", f"Не удалось сопоставить меру '{measure_name}'")
            continue

        _, month_limit, is_unlimited = _parse_limit_value(raw_limit)
        if month_limit is None and is_unlimited == 0:
            dq.add(raw_path.name, source_row_num, "INVALID_LIMIT_VALUE", f"Невалидный лимит '{raw_limit}'")

        yield {
            "action_type": action_type,
            "month_limit": "" if month_limit is None else month_limit,
            "is_unlimited": is_unlimited,
            "source_measure_name": measure_name,
        }


def _write_table(
    out_dir: Path,
    table_name: str,
    fieldnames: list[str],
    rows: Iterable[dict[str, object]],
) -> dict[str, object]:
    csv_path = out_dir / f"{table_name}.csv"
    row_count = write_csv_rows(csv_path, fieldnames, rows)

    parquet_path = csv_path.with_suffix(".parquet")
    parquet_ok, parquet_msg = maybe_write_parquet_from_csv(csv_path, parquet_path)
    if not parquet_ok and parquet_path.exists():
        parquet_path.unlink(missing_ok=True)

    return {
        "table": table_name,
        "csv": csv_path.name,
        "rows": row_count,
        "parquet": parquet_msg,
    }


def run(input_dir: Path, out_dir: Path) -> dict[str, object]:
    raw_dir = discover_input_layer(input_dir, RAW_DIRNAME)
    normalized_dir = out_dir / NORMALIZED_DIRNAME
    ensure_dir(normalized_dir)

    dq = DQCollector(stage="normalized")
    raw_files = _collect_raw_by_prefix(raw_dir)

    metrics: dict[str, object] = {
        "stage": "build_normalized",
        "input_raw_dir": str(raw_dir),
        "normalized_dir": str(normalized_dir),
        "tables": [],
    }

    # 01
    dim_rows = _build_dim_ls_profile(raw_files["01"], dq)
    metrics["tables"].append(
        _write_table(
            normalized_dir,
            "dim_ls_profile",
            ["ls_id"]
            + [field for _, field in LS_PROFILE_BINARY_FIELDS]
            + ["address_guid", "source_record_count"],
            dim_rows,
        )
    )

    # 02
    balance_rows = _build_balance_rows(raw_files["02"], dq)
    metrics["tables"].append(
        _write_table(
            normalized_dir,
            "fact_balance_monthly",
            ["ls_id", "period_month", "debt_start", "charge_amt", "paid_amt", "source_row_num"],
            balance_rows,
        )
    )

    # 03
    payment_rows = _build_payment_rows(raw_files["03"], dq)
    metrics["tables"].append(
        _write_table(
            normalized_dir,
            "fact_payment_txn",
            ["ls_id", "payment_date", "amount_rub", "payment_method", "is_reversal", "source_row_num"],
            payment_rows,
        )
    )

    # 04-13
    action_rows = _build_action_rows(raw_dir, dq)
    metrics["tables"].append(
        _write_table(
            normalized_dir,
            "fact_action_event",
            ["ls_id", "action_date", "action_type", "event_count"],
            action_rows,
        )
    )

    # 14
    limits_rows = _build_action_limits(raw_files["14"], dq)
    metrics["tables"].append(
        _write_table(
            normalized_dir,
            "dim_action_limit",
            ["action_type", "month_limit", "is_unlimited", "source_measure_name"],
            limits_rows,
        )
    )

    dq_metrics = _write_table(
        normalized_dir,
        "dq_issues",
        ["issue_id", "stage", "source_file", "source_row_num", "issue_type", "issue_detail", "ls_id_raw"],
        dq.as_rows(),
    )
    metrics["tables"].append(dq_metrics)
    metrics["dq_issue_count"] = dq_metrics["rows"]

    write_data_dictionary(normalized_dir / "data_dictionary.md", include_mart=True)

    metrics_path = normalized_dir / "load_metrics_normalized.json"
    with metrics_path.open("w", encoding="utf-8") as fh:
        json.dump(metrics, fh, ensure_ascii=False, indent=2)

    return metrics


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    metrics = run(Path(OUTPUT_DIR), Path(OUTPUT_DIR))
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
