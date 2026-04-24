from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

from .common import (
    add_months,
    date_to_str,
    decimal_to_str,
    discover_input_layer,
    ensure_dir,
    month_start,
    parse_as_of_month,
    parse_date_any,
    parse_decimal,
    read_csv_dict,
    write_csv_rows,
    maybe_write_parquet_from_csv,
)
from .constants import ACTION_TYPES, MART_DIRNAME, NORMALIZED_DIRNAME
from .data_dictionary import write_data_dictionary
from .settings import OUTPUT_DIR, PIPELINE_CONFIG


PROFILE_FIELDS = [
    "can_remote_disconnect",
    "has_phone",
    "has_benefits",
    "is_gasified",
    "is_city",
    "has_yaroblierc_receipt",
    "has_post_receipt",
    "has_e_receipt",
    "not_resident",
    "is_chd",
    "is_mkd",
    "is_dormitory",
    "meter_install_tambour",
    "meter_install_pole",
    "meter_install_apartment_house",
    "meter_install_stairwell",
]


def _int_or_zero(raw: str) -> int:
    value = (raw or "").strip()
    if not value:
        return 0
    try:
        return int(value)
    except ValueError:
        return 0


def _write_table(
    out_dir: Path,
    table_name: str,
    fieldnames: list[str],
    rows,
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


def run(input_dir: Path, out_dir: Path, as_of_month: str) -> dict[str, object]:
    normalized_dir = discover_input_layer(input_dir, NORMALIZED_DIRNAME)
    mart_dir = out_dir / MART_DIRNAME
    ensure_dir(mart_dir)

    as_of = parse_as_of_month(as_of_month)

    profile_by_ls: dict[str, dict[str, int | str]] = {}
    for row in read_csv_dict(normalized_dir / "dim_ls_profile.csv"):
        ls_id = row.get("ls_id", "")
        if not ls_id:
            continue
        profile: dict[str, int | str] = {"address_guid": row.get("address_guid", "")}
        for field in PROFILE_FIELDS:
            profile[field] = _int_or_zero(row.get(field, ""))
        profile["contact_channels_count"] = (
            int(profile["has_phone"])
            + int(profile["has_e_receipt"])
            + int(profile["has_post_receipt"])
            + int(profile["has_yaroblierc_receipt"])
        )
        profile_by_ls[ls_id] = profile

    balance_by_ls_month: dict[str, dict[date, tuple[Decimal, Decimal, Decimal]]] = defaultdict(dict)
    months_set: set[date] = set()
    for row in read_csv_dict(normalized_dir / "fact_balance_monthly.csv"):
        ls_id = row.get("ls_id", "")
        period_raw = row.get("period_month", "")
        if not ls_id or not period_raw:
            continue
        period = parse_date_any(period_raw)
        if period is None:
            continue
        period = month_start(period)

        debt = parse_decimal(row.get("debt_start", "")) or Decimal("0")
        charge = parse_decimal(row.get("charge_amt", "")) or Decimal("0")
        paid = parse_decimal(row.get("paid_amt", "")) or Decimal("0")

        balance_by_ls_month[ls_id][period] = (debt, charge, paid)
        months_set.add(period)

    payment_amt: dict[tuple[str, date], Decimal] = defaultdict(lambda: Decimal("0"))
    payment_cnt: dict[tuple[str, date], int] = defaultdict(int)
    payment_months: set[date] = set()
    for row in read_csv_dict(normalized_dir / "fact_payment_txn.csv"):
        ls_id = row.get("ls_id", "")
        d_raw = row.get("payment_date", "")
        if not ls_id or not d_raw:
            continue
        dt = parse_date_any(d_raw)
        if dt is None:
            continue
        month = month_start(dt)
        amt = parse_decimal(row.get("amount_rub", ""))
        if amt is None:
            continue
        payment_amt[(ls_id, month)] += amt
        payment_cnt[(ls_id, month)] += 1
        payment_months.add(month)

    action_total: dict[tuple[str, date], int] = defaultdict(int)
    action_type_count: dict[tuple[str, date, str], int] = defaultdict(int)
    action_types_present: dict[tuple[str, date], set[str]] = defaultdict(set)
    for row in read_csv_dict(normalized_dir / "fact_action_event.csv"):
        ls_id = row.get("ls_id", "")
        d_raw = row.get("action_date", "")
        action_type = row.get("action_type", "")
        if not ls_id or not d_raw or not action_type:
            continue
        dt = parse_date_any(d_raw)
        if dt is None:
            continue
        month = month_start(dt)
        cnt = _int_or_zero(row.get("event_count", ""))
        action_total[(ls_id, month)] += cnt
        action_type_count[(ls_id, month, action_type)] += cnt
        action_types_present[(ls_id, month)].add(action_type)

    months_sorted = sorted(months_set)
    if not months_sorted:
        raise ValueError("No months found in fact_balance_monthly")

    max_payment_month = max(payment_months) if payment_months else None

    fieldnames = [
        "ls_id",
        "month",
        "debt_start_t",
        "charge_amt_t",
        "paid_amt_t",
        "debt_change_1m",
    ]
    fieldnames += PROFILE_FIELDS + ["contact_channels_count"]
    fieldnames += [
        "payment_txn_count_l1m",
        "payment_txn_count_l3m",
        "payment_txn_count_l6m",
        "payment_amount_l1m",
        "payment_amount_l3m",
        "payment_amount_l6m",
        "action_total_l1m",
        "action_total_l3m",
        "action_total_l6m",
        "action_unique_types_l1m",
        "action_unique_types_l3m",
        "action_unique_types_l6m",
    ]
    for action_type in ACTION_TYPES:
        fieldnames.extend(
            [
                f"action_{action_type}_l1m",
                f"action_{action_type}_l3m",
                f"action_{action_type}_l6m",
            ]
        )

    fieldnames += [
        "paid_any_next_month",
        "paid_amount_next_month",
        "recovery_ratio_next_month",
    ]

    def rows_iter():
        for ls_id in sorted(balance_by_ls_month.keys(), key=lambda x: int(x)):
            profile = profile_by_ls.get(ls_id, {})
            for idx, month in enumerate(months_sorted):
                if month > as_of:
                    continue
                if month not in balance_by_ls_month[ls_id]:
                    continue

                debt_t, charge_t, paid_t = balance_by_ls_month[ls_id][month]

                prev_month = add_months(month, -1)
                prev_debt = None
                if prev_month in balance_by_ls_month[ls_id]:
                    prev_debt = balance_by_ls_month[ls_id][prev_month][0]

                out: dict[str, object] = {
                    "ls_id": ls_id,
                    "month": month.isoformat(),
                    "debt_start_t": decimal_to_str(debt_t),
                    "charge_amt_t": decimal_to_str(charge_t),
                    "paid_amt_t": decimal_to_str(paid_t),
                    "debt_change_1m": decimal_to_str(debt_t - prev_debt) if prev_debt is not None else "",
                }

                for field in PROFILE_FIELDS:
                    out[field] = int(profile.get(field, 0))
                out["contact_channels_count"] = int(profile.get("contact_channels_count", 0))

                for window in (1, 3, 6):
                    window_start_idx = max(0, idx - window + 1)
                    win_months = months_sorted[window_start_idx : idx + 1]

                    pay_cnt_val = 0
                    pay_amt_val = Decimal("0")
                    action_total_val = 0
                    unique_action_types: set[str] = set()
                    action_type_sums = {action: 0 for action in ACTION_TYPES}

                    for m in win_months:
                        pay_cnt_val += payment_cnt[(ls_id, m)]
                        pay_amt_val += payment_amt[(ls_id, m)]
                        action_total_val += action_total[(ls_id, m)]
                        unique_action_types.update(action_types_present[(ls_id, m)])
                        for action in ACTION_TYPES:
                            action_type_sums[action] += action_type_count[(ls_id, m, action)]

                    out[f"payment_txn_count_l{window}m"] = pay_cnt_val
                    out[f"payment_amount_l{window}m"] = decimal_to_str(pay_amt_val)
                    out[f"action_total_l{window}m"] = action_total_val
                    out[f"action_unique_types_l{window}m"] = len(unique_action_types)
                    for action in ACTION_TYPES:
                        out[f"action_{action}_l{window}m"] = action_type_sums[action]

                next_month = add_months(month, 1)
                if max_payment_month is None or next_month > max_payment_month:
                    out["paid_any_next_month"] = ""
                    out["paid_amount_next_month"] = ""
                    out["recovery_ratio_next_month"] = ""
                else:
                    paid_next = payment_amt[(ls_id, next_month)]
                    out["paid_any_next_month"] = 1 if paid_next > 0 else 0
                    out["paid_amount_next_month"] = decimal_to_str(paid_next)
                    denom = debt_t + charge_t
                    ratio = Decimal("0") if denom <= 0 else (paid_next / denom)
                    out["recovery_ratio_next_month"] = decimal_to_str(ratio)

                yield out

    metrics: dict[str, object] = {
        "stage": "build_mart",
        "input_normalized_dir": str(normalized_dir),
        "mart_dir": str(mart_dir),
        "as_of_month": as_of.isoformat(),
    }

    table_metrics = _write_table(mart_dir, "mart_ls_month", fieldnames, rows_iter())
    metrics["tables"] = [table_metrics]

    write_data_dictionary(mart_dir / "data_dictionary.md", include_mart=True)

    metrics_path = mart_dir / "load_metrics_mart.json"
    with metrics_path.open("w", encoding="utf-8") as fh:
        json.dump(metrics, fh, ensure_ascii=False, indent=2)

    return metrics


def _default_as_of_month() -> str:
    configured_month = PIPELINE_CONFIG.get("month")
    if configured_month:
        return str(configured_month)

    normalized_dir = discover_input_layer(Path(OUTPUT_DIR), NORMALIZED_DIRNAME)
    latest_month: date | None = None
    for row in read_csv_dict(normalized_dir / "fact_balance_monthly.csv"):
        period = parse_date_any(row.get("period_month", ""))
        if period is None:
            continue
        period = month_start(period)
        if latest_month is None or period > latest_month:
            latest_month = period

    if latest_month is None:
        raise ValueError("Cannot detect latest month from fact_balance_monthly.csv")
    return latest_month.strftime("%Y-%m")


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    metrics = run(Path(OUTPUT_DIR), Path(OUTPUT_DIR), _default_as_of_month())
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
