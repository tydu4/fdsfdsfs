from __future__ import annotations

import json
import random
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from itertools import combinations
from pathlib import Path
from typing import Iterable

from .common import (
    discover_input_layer,
    ensure_dir,
    month_start,
    maybe_write_parquet_from_csv,
    parse_as_of_month,
    parse_date_any,
    read_csv_dict,
    write_csv_rows,
)
from .constants import ACTION_TYPES, NORMALIZED_DIRNAME
from .settings import OUTPUT_DIR, PIPELINE_CONFIG


MART_DIRNAME = "mart"
SEGMENTATION_DIRNAME = "segmentation"
UPLIFT_DIRNAME = "uplift"

DEFAULT_MEASURES = (
    *ACTION_TYPES,
)

INFORMING_ACTIONS = {
    "email",
    "sms",
    "autodial",
    "operator_call",
    "claim",
    "field_visit",
    "restriction_notice",
}

MEASURE_PRIORITY = {
    "email": 1,
    "sms": 2,
    "autodial": 3,
    "operator_call": 4,
    "claim": 5,
    "field_visit": 6,
    "restriction_notice": 7,
    "restriction": 8,
    "court_order_request": 9,
    "court_order_received": 10,
}

MAX_COMBO_ACTIONS = 2
COMBO_SECOND_ACTION_WEIGHT = 0.7

FEATURE_COLUMNS = [
    "debt_start_t",
    "charge_amt_t",
    "paid_amt_t",
    "debt_change_1m",
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
    "contact_channels_count",
    "has_phone",
    "has_e_receipt",
    "can_remote_disconnect",
    "has_benefits",
    "is_city",
    "is_mkd",
]


@dataclass
class MeasureTrainingData:
    x_train: list[list[float]]
    t_train: list[int]
    y_train: list[int]
    x_score: list[list[float]]
    score_rows: list[dict[str, str]]


@dataclass
class RecommendationContext:
    debt_start: float
    debt_age_months: int
    charge_amt_t: float
    payment_amount_l1m: float
    payment_txn_count_l1m: int
    has_phone: int
    has_e_receipt: int
    can_remote_disconnect: int
    action_history: dict[str, int]


@dataclass
class CandidatePlan:
    actions: tuple[str, ...]
    score: float
    kind: str


class ConstantProbModel:
    def __init__(self, p1: float) -> None:
        self.p1 = min(max(float(p1), 0.0), 1.0)

    def predict_proba(self, x):
        import numpy as np  # type: ignore

        n = len(x)
        out = np.zeros((n, 2), dtype=float)
        out[:, 1] = self.p1
        out[:, 0] = 1.0 - self.p1
        return out


class TrainedLogitModel:
    def __init__(self, pipeline, feature_names: list[str]) -> None:
        self.pipeline = pipeline
        self.feature_names = feature_names

    def predict_proba(self, x):
        return self.pipeline.predict_proba(x)

    def coefficients(self) -> dict[str, float]:
        scaler = self.pipeline.named_steps.get("scaler")
        logreg = self.pipeline.named_steps.get("logreg")
        if logreg is None:
            return {}

        coef = logreg.coef_[0]
        scales = getattr(scaler, "scale_", None)
        if scales is None:
            scales = [1.0] * len(coef)

        out: dict[str, float] = {}
        for name, c, s in zip(self.feature_names, coef, scales):
            denom = float(s) if float(s) != 0.0 else 1.0
            out[name] = float(c) / denom
        return out


def _to_float(raw: str) -> float:
    value = (raw or "").strip()
    if not value:
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


def _to_int(raw: str) -> int:
    value = (raw or "").strip()
    if not value:
        return 0
    try:
        return int(float(value))
    except ValueError:
        return 0


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


def _detect_latest_month(mart_csv_path: Path) -> date:
    latest: date | None = None
    for row in read_csv_dict(mart_csv_path):
        month = parse_date_any(row.get("month", ""))
        if month is None:
            continue
        if latest is None or month > latest:
            latest = month
    if latest is None:
        raise ValueError("Cannot detect latest month from mart_ls_month.csv")
    return latest


def _parse_measures(raw: str | None) -> list[str]:
    if not raw:
        return list(DEFAULT_MEASURES)
    parts = [x.strip() for x in raw.split(",")]
    out: list[str] = []
    for item in parts:
        if not item:
            continue
        if item not in ACTION_TYPES:
            continue
        if item in out:
            continue
        out.append(item)
    if not out:
        return list(DEFAULT_MEASURES)
    return out


def _find_hybrid_assignments(input_dir: Path, mart_dir: Path) -> Path | None:
    candidates = [
        input_dir / SEGMENTATION_DIRNAME / "segment_hybrid_assignments.csv",
        mart_dir.parent / SEGMENTATION_DIRNAME / "segment_hybrid_assignments.csv",
        input_dir / "segment_hybrid_assignments.csv",
    ]
    for path in candidates:
        if path.exists() and path.is_file():
            return path
    return None


def _load_gate_lookup(input_dir: Path, mart_dir: Path) -> dict[tuple[str, str], dict[str, str]]:
    path = _find_hybrid_assignments(input_dir, mart_dir)
    if path is None:
        return {}

    out: dict[tuple[str, str], dict[str, str]] = {}
    for row in read_csv_dict(path):
        ls_id = (row.get("ls_id", "") or "").strip()
        month = (row.get("month", "") or "").strip()
        if not ls_id or not month:
            continue
        out[(ls_id, month)] = {
            "business_gate": (row.get("business_gate", "") or "").strip(),
            "gate_ml_eligible": (row.get("gate_ml_eligible", "") or "").strip(),
            "hybrid_segment": (row.get("hybrid_segment", "") or "").strip(),
        }
    return out


def _load_action_limits(input_dir: Path) -> dict[str, int | None]:
    normalized_dir = discover_input_layer(input_dir, NORMALIZED_DIRNAME)
    limits_path = normalized_dir / "dim_action_limit.csv"
    limits: dict[str, int | None] = {action: None for action in ACTION_TYPES}
    if not limits_path.exists():
        return limits

    for row in read_csv_dict(limits_path):
        action = (row.get("action_type", "") or "").strip()
        if action not in ACTION_TYPES:
            continue
        is_unlimited = (row.get("is_unlimited", "") or "").strip() == "1"
        if is_unlimited:
            limits[action] = None
            continue
        raw_limit = (row.get("month_limit", "") or "").strip()
        limit_value = max(_to_int(raw_limit), 0)
        limits[action] = limit_value
    return limits


def _load_action_history(input_dir: Path, target_month: date) -> dict[str, dict[str, int]]:
    normalized_dir = discover_input_layer(input_dir, NORMALIZED_DIRNAME)
    history_path = normalized_dir / "fact_action_event.csv"
    history: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    if not history_path.exists():
        return {}

    for row in read_csv_dict(history_path):
        ls_id = (row.get("ls_id", "") or "").strip()
        action_type = (row.get("action_type", "") or "").strip()
        dt = parse_date_any(row.get("action_date", ""))
        if not ls_id or not action_type or dt is None:
            continue
        action_month = month_start(dt)
        if action_month >= target_month:
            continue
        history[ls_id][action_type] += max(_to_int(row.get("event_count", "")), 0)

    return {ls_id: dict(action_map) for ls_id, action_map in history.items()}


def _load_debt_age_lookup(mart_csv_path: Path, target_month: date) -> dict[tuple[str, str], int]:
    rows_by_ls: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for row in read_csv_dict(mart_csv_path):
        ls_id = (row.get("ls_id", "") or "").strip()
        month = parse_date_any(row.get("month", ""))
        if not ls_id or month is None or month > target_month:
            continue
        rows_by_ls[ls_id].append((month, _to_float(row.get("debt_start_t", ""))))

    debt_age_lookup: dict[tuple[str, str], int] = {}
    for ls_id, rows in rows_by_ls.items():
        streak = 0
        for month, debt in sorted(rows, key=lambda x: x[0]):
            if debt > 0:
                streak += 1
            else:
                streak = 0
            debt_age_lookup[(ls_id, month.isoformat())] = streak
    return debt_age_lookup


def _is_gate_eligible(gate_ml_eligible: str) -> bool:
    gate_value = (gate_ml_eligible or "").strip()
    return gate_value not in {"0", "false", "False"}


def _parse_measures_in_plan(raw_measure: str) -> list[str]:
    value = (raw_measure or "").strip()
    if not value:
        return []
    return [part for part in value.split("+") if part]


def _is_limit_feasible(actions: tuple[str, ...], remaining_limits: dict[str, int | None]) -> bool:
    action_counts: dict[str, int] = defaultdict(int)
    for action in actions:
        action_counts[action] += 1
    for action, need in action_counts.items():
        limit = remaining_limits.get(action)
        if limit is None:
            continue
        if limit < need:
            return False
    return True


def _consume_limits(actions: tuple[str, ...], remaining_limits: dict[str, int | None]) -> None:
    for action in actions:
        limit = remaining_limits.get(action)
        if limit is None:
            continue
        remaining_limits[action] = max(limit - 1, 0)


def _plan_sort_key(plan: CandidatePlan) -> tuple[float, int, tuple[int, ...]]:
    plan_priority = tuple(MEASURE_PRIORITY.get(action, 999) for action in plan.actions)
    return (-plan.score, len(plan.actions), plan_priority)


def _admissibility(measure: str, context: RecommendationContext) -> tuple[bool, str]:
    debt = context.debt_start
    debt_age = context.debt_age_months
    has_phone = context.has_phone == 1
    has_e_receipt = context.has_e_receipt == 1
    can_remote_disconnect = context.can_remote_disconnect == 1
    payment_recent = context.payment_amount_l1m > 0 or context.payment_txn_count_l1m > 0

    action_history = context.action_history
    informing_count = sum(int(action_history.get(action, 0)) for action in INFORMING_ACTIONS)
    informing_done = informing_count > 0
    failed_informing = informing_done and not payment_recent
    phone_attempted = int(action_history.get("operator_call", 0)) + int(action_history.get("autodial", 0)) > 0
    failed_phone_contact = phone_attempted and not payment_recent
    restriction_done = int(action_history.get("restriction", 0)) > 0
    court_request_done = int(action_history.get("court_order_request", 0)) > 0

    if debt <= 0:
        return False, "debt_non_positive"

    if measure == "email":
        if debt_age < 1:
            return False, "min_debt_age_1m"
        if debt < 500:
            return False, "min_debt_500"
        if not has_e_receipt:
            return False, "requires_email_channel"
        return True, "ok"

    if measure == "sms":
        if debt_age < 1 or debt_age > 2:
            return False, "debt_age_1_2m"
        if debt < 500 or debt > 2000:
            return False, "debt_500_2000"
        if not has_phone:
            return False, "requires_mobile_phone"
        return True, "ok"

    if measure == "autodial":
        if debt_age < 2 or debt_age > 6:
            return False, "debt_age_2_6m"
        if debt < 500 or debt > 1500:
            return False, "debt_500_1500"
        if not has_phone:
            return False, "requires_phone"
        return True, "ok"

    if measure == "operator_call":
        if debt_age < 2 or debt_age > 6:
            return False, "debt_age_2_6m"
        if debt < 1500:
            return False, "min_debt_1500"
        if not has_phone:
            return False, "requires_phone"
        return True, "ok"

    if measure == "claim":
        if debt_age < 2 or debt_age > 6:
            return False, "debt_age_2_6m"
        if debt < 500:
            return False, "min_debt_500"
        if has_phone and not failed_phone_contact:
            return False, "requires_no_phone_or_failed_contact"
        return True, "ok"

    if measure == "field_visit":
        if debt_age < 2 or debt_age > 6:
            return False, "debt_age_2_6m"
        if debt < 1500:
            return False, "min_debt_1500"
        if has_phone and not failed_phone_contact:
            return False, "requires_failed_remote_contact"
        return True, "ok"

    if measure == "restriction_notice":
        if debt_age < 1:
            return False, "min_debt_age_1m"
        if debt < 1500:
            return False, "min_debt_1500"
        normative_proxy = max(context.charge_amt_t, 1.0)
        if debt <= 2.0 * normative_proxy:
            return False, "debt_not_gt_2x_normative_proxy"
        if not failed_informing:
            return False, "requires_failed_informing"
        return True, "ok"

    if measure == "restriction":
        if debt_age < 1:
            return False, "min_debt_age_1m"
        if debt < 1500:
            return False, "min_debt_1500"
        if not can_remote_disconnect:
            return False, "requires_remote_disconnect_capability"
        if not informing_done:
            return False, "requires_informing_completed"
        return True, "ok"

    if measure == "court_order_request":
        tier_ok = (
            (debt_age >= 4 and debt >= 10000)
            or (debt_age >= 6 and debt >= 4000)
            or (debt_age >= 11 and debt >= 2000)
        )
        if not tier_ok:
            return False, "court_age_amount_tier_not_met"
        if not (restriction_done or not can_remote_disconnect):
            return False, "requires_restriction_or_impossible"
        return True, "ok"

    if measure == "court_order_received":
        if not court_request_done:
            return False, "requires_prior_court_request"
        return True, "ok"

    return True, "ok"


def _is_combo_cascade_valid(actions: tuple[str, ...]) -> bool:
    action_set = set(actions)
    if "court_order_received" in action_set and "court_order_request" not in action_set:
        return False
    return True


def _build_candidates(
    measure_scores: dict[str, float],
    admissible_measures: set[str],
) -> tuple[list[CandidatePlan], bool]:
    positive = [(measure, score) for measure, score in measure_scores.items() if measure in admissible_measures and score > 0]
    positive.sort(key=lambda x: x[1], reverse=True)

    candidates: list[CandidatePlan] = []
    for measure, score in positive:
        candidates.append(CandidatePlan(actions=(measure,), score=score, kind="single"))

    if MAX_COMBO_ACTIONS >= 2 and len(positive) >= 2:
        for left, right in combinations(positive, 2):
            actions_scored = sorted([left, right], key=lambda x: x[1], reverse=True)
            actions = tuple(item[0] for item in actions_scored)
            if not _is_combo_cascade_valid(actions):
                continue
            combo_score = float(actions_scored[0][1]) + COMBO_SECOND_ACTION_WEIGHT * float(actions_scored[1][1])
            candidates.append(CandidatePlan(actions=actions, score=combo_score, kind="combo"))

    # Keep unique action sets with max score.
    dedup: dict[tuple[str, ...], CandidatePlan] = {}
    for plan in candidates:
        key = tuple(plan.actions)
        prev = dedup.get(key)
        if prev is None or plan.score > prev.score:
            dedup[key] = plan
    ordered = sorted(dedup.values(), key=_plan_sort_key)
    has_positive_admissible = bool(positive)
    return ordered, has_positive_admissible


def _subsample(
    x: list[list[float]],
    t: list[int],
    y: list[int],
    max_rows: int,
    random_state: int,
) -> tuple[list[list[float]], list[int], list[int]]:
    n = len(x)
    if n <= max_rows:
        return x, t, y

    rng = random.Random(random_state)
    idx = list(range(n))
    rng.shuffle(idx)
    idx = idx[:max_rows]

    x_out = [x[i] for i in idx]
    t_out = [t[i] for i in idx]
    y_out = [y[i] for i in idx]
    return x_out, t_out, y_out


def _fit_binary_model(x: list[list[float]], y: list[int], random_state: int, feature_names: list[str]):
    import numpy as np  # type: ignore
    from sklearn.linear_model import LogisticRegression  # type: ignore
    from sklearn.pipeline import Pipeline  # type: ignore
    from sklearn.preprocessing import StandardScaler  # type: ignore

    if len(x) == 0:
        return ConstantProbModel(0.0)

    y_set = set(int(v) for v in y)
    if len(y_set) < 2:
        p1 = float(sum(y) / max(len(y), 1))
        return ConstantProbModel(p1)

    x_arr = np.asarray(x, dtype=float)
    y_arr = np.asarray(y, dtype=int)

    pipe = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "logreg",
                LogisticRegression(
                    random_state=random_state,
                    max_iter=300,
                    solver="lbfgs",
                ),
            ),
        ]
    )
    pipe.fit(x_arr, y_arr)
    return TrainedLogitModel(pipe, feature_names)


def _collect_measure_data(
    mart_csv_path: Path,
    score_month: date,
    measure: str,
    feature_columns: list[str],
) -> MeasureTrainingData:
    treat_col = f"action_{measure}_l1m"

    x_train: list[list[float]] = []
    t_train: list[int] = []
    y_train: list[int] = []

    x_score: list[list[float]] = []
    score_rows: list[dict[str, str]] = []

    for row in read_csv_dict(mart_csv_path):
        month = parse_date_any(row.get("month", ""))
        if month is None:
            continue

        x = [_to_float(row.get(col, "")) for col in feature_columns]
        treat = 1 if _to_float(row.get(treat_col, "")) > 0 else 0

        if month < score_month:
            y_raw = (row.get("paid_any_next_month", "") or "").strip()
            if y_raw not in {"0", "1"}:
                continue
            y = int(y_raw)
            x_train.append(x)
            t_train.append(treat)
            y_train.append(y)
        elif month == score_month:
            x_score.append(x)
            score_rows.append(row)

    return MeasureTrainingData(
        x_train=x_train,
        t_train=t_train,
        y_train=y_train,
        x_score=x_score,
        score_rows=score_rows,
    )


def run(
    input_dir: Path,
    out_dir: Path,
    score_month: str | None,
    measures_raw: str | None,
    max_train_rows: int,
    random_state: int,
) -> dict[str, object]:
    import numpy as np  # type: ignore

    mart_dir = discover_input_layer(input_dir, MART_DIRNAME)
    mart_csv_path = mart_dir / "mart_ls_month.csv"
    if not mart_csv_path.exists():
        raise FileNotFoundError(f"File not found: {mart_csv_path}")

    uplift_dir = out_dir / UPLIFT_DIRNAME
    ensure_dir(uplift_dir)

    measures = _parse_measures(measures_raw)
    if score_month:
        target_month = parse_as_of_month(score_month)
    else:
        target_month = _detect_latest_month(mart_csv_path)

    gate_lookup = _load_gate_lookup(input_dir, mart_dir)
    action_limits = _load_action_limits(input_dir)
    remaining_limits: dict[str, int | None] = {action: action_limits.get(action) for action in ACTION_TYPES}
    action_history_lookup = _load_action_history(input_dir, target_month)
    debt_age_lookup = _load_debt_age_lookup(mart_csv_path, target_month)

    score_rows_long: list[dict[str, object]] = []
    measure_summary: list[dict[str, object]] = []
    feature_importance_rows: list[dict[str, object]] = []

    recommendations: dict[tuple[str, str], dict[str, object]] = {}

    for measure_idx, measure in enumerate(measures):
        data = _collect_measure_data(mart_csv_path, target_month, measure, FEATURE_COLUMNS)

        x_train, t_train, y_train = _subsample(
            data.x_train,
            data.t_train,
            data.y_train,
            max_rows=max_train_rows,
            random_state=random_state + (measure_idx + 1) * 1009,
        )

        if len(x_train) == 0:
            continue

        # models
        prop_model = _fit_binary_model(x_train, t_train, random_state + 11 + measure_idx, FEATURE_COLUMNS)

        x_treated = [x for x, t in zip(x_train, t_train) if t == 1]
        y_treated = [y for y, t in zip(y_train, t_train) if t == 1]
        x_control = [x for x, t in zip(x_train, t_train) if t == 0]
        y_control = [y for y, t in zip(y_train, t_train) if t == 0]

        out_t_model = _fit_binary_model(x_treated, y_treated, random_state + 23 + measure_idx, FEATURE_COLUMNS)
        out_c_model = _fit_binary_model(x_control, y_control, random_state + 31 + measure_idx, FEATURE_COLUMNS)

        x_train_arr = np.asarray(x_train, dtype=float)
        t_arr = np.asarray(t_train, dtype=float)
        y_arr = np.asarray(y_train, dtype=float)

        e = prop_model.predict_proba(x_train_arr)[:, 1]
        e = np.clip(e, 0.01, 0.99)
        mu1 = out_t_model.predict_proba(x_train_arr)[:, 1]
        mu0 = out_c_model.predict_proba(x_train_arr)[:, 1]
        uplift_train = mu1 - mu0

        dr = uplift_train + t_arr * (y_arr - mu1) / e - (1.0 - t_arr) * (y_arr - mu0) / (1.0 - e)
        ate_dr = float(np.mean(dr)) if len(dr) > 0 else 0.0

        treated_rate = float(np.mean(y_treated)) if y_treated else 0.0
        control_rate = float(np.mean(y_control)) if y_control else 0.0
        naive_ate = treated_rate - control_rate

        # Score selected month
        if data.x_score:
            x_score_arr = np.asarray(data.x_score, dtype=float)
            p1 = out_t_model.predict_proba(x_score_arr)[:, 1]
            p0 = out_c_model.predict_proba(x_score_arr)[:, 1]
            uplift = p1 - p0

            for row, p1_i, p0_i, u_i in zip(data.score_rows, p1, p0, uplift):
                ls_id = (row.get("ls_id", "") or "").strip()
                month_str = (row.get("month", "") or "").strip()
                if not ls_id or not month_str:
                    continue

                gate = gate_lookup.get((ls_id, month_str), {})
                business_gate = gate.get("business_gate", "")
                gate_ml_eligible = gate.get("gate_ml_eligible", "")
                hybrid_segment = gate.get("hybrid_segment", "")

                debt_age_months = debt_age_lookup.get((ls_id, month_str), 0)
                context = RecommendationContext(
                    debt_start=_to_float(row.get("debt_start_t", "")),
                    debt_age_months=debt_age_months,
                    charge_amt_t=_to_float(row.get("charge_amt_t", "")),
                    payment_amount_l1m=_to_float(row.get("payment_amount_l1m", "")),
                    payment_txn_count_l1m=_to_int(row.get("payment_txn_count_l1m", "")),
                    has_phone=_to_int(row.get("has_phone", "")),
                    has_e_receipt=_to_int(row.get("has_e_receipt", "")),
                    can_remote_disconnect=_to_int(row.get("can_remote_disconnect", "")),
                    action_history=action_history_lookup.get(ls_id, {}),
                )
                is_admissible, admissibility_reason = _admissibility(measure, context)

                out_row = {
                    "ls_id": ls_id,
                    "month": month_str,
                    "measure": measure,
                    "uplift_score": round(float(u_i), 8),
                    "p_if_treated": round(float(p1_i), 8),
                    "p_if_control": round(float(p0_i), 8),
                    "business_gate": business_gate,
                    "gate_ml_eligible": gate_ml_eligible,
                    "hybrid_segment": hybrid_segment,
                    "is_admissible": 1 if is_admissible else 0,
                    "admissibility_reason": admissibility_reason,
                    "debt_start_t": round(context.debt_start, 2),
                    "debt_age_months": context.debt_age_months,
                }
                score_rows_long.append(out_row)

                rec_key = (ls_id, month_str)
                if rec_key not in recommendations:
                    recommendations[rec_key] = {
                        "ls_id": ls_id,
                        "month": month_str,
                        "business_gate": business_gate,
                        "gate_ml_eligible": gate_ml_eligible,
                        "hybrid_segment": hybrid_segment,
                        "measure_scores": {},
                        "admissibility": {},
                        "admissibility_reasons": {},
                    }

                rec = recommendations[rec_key]
                u_val = float(u_i)
                measure_scores = rec["measure_scores"]
                admissibility = rec["admissibility"]
                admissibility_reasons = rec["admissibility_reasons"]
                if isinstance(measure_scores, dict):
                    measure_scores[measure] = u_val
                if isinstance(admissibility, dict):
                    admissibility[measure] = bool(is_admissible)
                if isinstance(admissibility_reasons, dict):
                    admissibility_reasons[measure] = admissibility_reason

        measure_summary.append(
            {
                "measure": measure,
                "train_rows": len(x_train),
                "train_treated_rows": int(sum(t_train)),
                "train_treated_share": round(float(sum(t_train) / max(len(t_train), 1)), 6),
                "train_outcome_rate": round(float(sum(y_train) / max(len(y_train), 1)), 6),
                "treated_outcome_rate": round(treated_rate, 6),
                "control_outcome_rate": round(control_rate, 6),
                "naive_ate": round(float(naive_ate), 8),
                "ate_dr": round(float(ate_dr), 8),
                "avg_pred_uplift_train": round(float(np.mean(uplift_train)), 8),
                "score_rows": len(data.x_score),
            }
        )

        # Feature importance (coefs diff)
        coef_t = out_t_model.coefficients() if isinstance(out_t_model, TrainedLogitModel) else {}
        coef_c = out_c_model.coefficients() if isinstance(out_c_model, TrainedLogitModel) else {}
        features_union = set(coef_t.keys()) | set(coef_c.keys())
        ranked = []
        for feat in features_union:
            c_t = float(coef_t.get(feat, 0.0))
            c_c = float(coef_c.get(feat, 0.0))
            ranked.append((feat, c_t, c_c, abs(c_t - c_c)))
        ranked.sort(key=lambda x: x[3], reverse=True)

        for feat, c_t, c_c, diff_abs in ranked[:10]:
            feature_importance_rows.append(
                {
                    "measure": measure,
                    "feature": feat,
                    "coef_treated_model": round(c_t, 8),
                    "coef_control_model": round(c_c, 8),
                    "coef_diff_abs": round(diff_abs, 8),
                }
            )

    # Build recommendation table (admissibility + cascade + monthly limits)
    recommendation_jobs: list[dict[str, object]] = []
    for _, rec in sorted(recommendations.items(), key=lambda x: (x[0][1], int(x[0][0]))):
        gate_ml_eligible = str(rec.get("gate_ml_eligible", "")).strip()
        eligible = _is_gate_eligible(gate_ml_eligible)
        measure_scores = rec.get("measure_scores", {})
        admissibility = rec.get("admissibility", {})

        if not isinstance(measure_scores, dict):
            measure_scores = {}
        if not isinstance(admissibility, dict):
            admissibility = {}

        admissible_measures = {measure for measure, is_ok in admissibility.items() if bool(is_ok)}
        admissible_scores = [float(score) for measure, score in measure_scores.items() if measure in admissible_measures]
        best_admissible_score = max(admissible_scores) if admissible_scores else None

        if not eligible:
            base_reason = "gate_ml_ineligible"
            candidates: list[CandidatePlan] = []
            has_positive_admissible = False
        elif not admissible_measures:
            base_reason = "no_admissible_measure"
            candidates = []
            has_positive_admissible = False
        else:
            candidates, has_positive_admissible = _build_candidates(
                {measure: float(score) for measure, score in measure_scores.items()},
                admissible_measures,
            )
            if not has_positive_admissible:
                base_reason = "non_positive_uplift"
            else:
                base_reason = "month_limit_exhausted"

        recommendation_jobs.append(
            {
                "rec": rec,
                "base_reason": base_reason,
                "candidates": candidates,
                "best_admissible_score": best_admissible_score,
                "top_candidate_score": float(candidates[0].score) if candidates else -999.0,
                "has_positive_admissible": has_positive_admissible,
            }
        )

    recommendations_rows: list[dict[str, object]] = []
    recommendation_jobs.sort(
        key=lambda job: (
            -float(job.get("top_candidate_score", -999.0)),
            int(str(job["rec"].get("ls_id", "0")) or "0"),
        )
    )

    for job in recommendation_jobs:
        rec = job["rec"]
        candidates = job.get("candidates", [])
        if not isinstance(candidates, list):
            candidates = []
        base_reason = str(job.get("base_reason", "non_positive_uplift"))

        chosen_plan: CandidatePlan | None = None
        chosen_idx = -1
        limit_blocked_any = False
        for idx, plan in enumerate(candidates):
            if not isinstance(plan, CandidatePlan):
                continue
            if _is_limit_feasible(plan.actions, remaining_limits):
                chosen_plan = plan
                chosen_idx = idx
                break
            limit_blocked_any = True

        if chosen_plan is not None:
            _consume_limits(chosen_plan.actions, remaining_limits)
            recommended_measure = "+".join(chosen_plan.actions)
            recommended_uplift = round(float(chosen_plan.score), 8)
            if chosen_plan.kind == "combo":
                recommendation_reason = "max_uplift_combo_with_limits" if chosen_idx > 0 else "max_uplift_combo"
            else:
                recommendation_reason = "max_uplift_with_limits" if chosen_idx > 0 else "max_uplift"
        else:
            recommended_measure = ""
            if base_reason == "non_positive_uplift":
                best_score = job.get("best_admissible_score")
                recommended_uplift = "" if best_score is None else round(float(best_score), 8)
            else:
                recommended_uplift = ""
            if base_reason == "month_limit_exhausted" and limit_blocked_any:
                recommendation_reason = "month_limit_exhausted"
            else:
                recommendation_reason = base_reason

        second_plan: CandidatePlan | None = None
        for plan in candidates:
            if not isinstance(plan, CandidatePlan):
                continue
            if chosen_plan is not None and plan.actions == chosen_plan.actions:
                continue
            second_plan = plan
            break

        second_measure = "" if second_plan is None else "+".join(second_plan.actions)
        second_uplift = "" if second_plan is None else round(float(second_plan.score), 8)

        recommendations_rows.append(
            {
                "ls_id": rec["ls_id"],
                "month": rec["month"],
                "business_gate": rec.get("business_gate", ""),
                "gate_ml_eligible": rec.get("gate_ml_eligible", ""),
                "hybrid_segment": rec.get("hybrid_segment", ""),
                "recommended_measure": recommended_measure,
                "recommended_uplift": recommended_uplift,
                "second_measure": second_measure,
                "second_uplift": second_uplift,
                "recommendation_reason": recommendation_reason,
                "recommended_actions_count": len(_parse_measures_in_plan(recommended_measure)),
            }
        )

    recommendations_rows.sort(key=lambda row: (str(row.get("month", "")), int(str(row.get("ls_id", "0")) or "0")))

    limit_usage: dict[str, int] = defaultdict(int)
    for row in recommendations_rows:
        for action in _parse_measures_in_plan(str(row.get("recommended_measure", ""))):
            limit_usage[action] += 1

    table_metrics: list[dict[str, object]] = []
    table_metrics.append(
        _write_table(
            uplift_dir,
            "uplift_measure_scores",
            [
                "ls_id",
                "month",
                "measure",
                "uplift_score",
                "p_if_treated",
                "p_if_control",
                "business_gate",
                "gate_ml_eligible",
                "hybrid_segment",
                "is_admissible",
                "admissibility_reason",
                "debt_start_t",
                "debt_age_months",
            ],
            score_rows_long,
        )
    )

    table_metrics.append(
        _write_table(
            uplift_dir,
            "uplift_recommendations",
            [
                "ls_id",
                "month",
                "business_gate",
                "gate_ml_eligible",
                "hybrid_segment",
                "recommended_measure",
                "recommended_uplift",
                "second_measure",
                "second_uplift",
                "recommendation_reason",
                "recommended_actions_count",
            ],
            recommendations_rows,
        )
    )

    table_metrics.append(
        _write_table(
            uplift_dir,
            "uplift_measure_summary",
            [
                "measure",
                "train_rows",
                "train_treated_rows",
                "train_treated_share",
                "train_outcome_rate",
                "treated_outcome_rate",
                "control_outcome_rate",
                "naive_ate",
                "ate_dr",
                "avg_pred_uplift_train",
                "score_rows",
            ],
            measure_summary,
        )
    )

    table_metrics.append(
        _write_table(
            uplift_dir,
            "uplift_feature_importance",
            ["measure", "feature", "coef_treated_model", "coef_control_model", "coef_diff_abs"],
            feature_importance_rows,
        )
    )

    metrics: dict[str, object] = {
        "stage": "build_uplift_prototype",
        "input_mart_dir": str(mart_dir),
        "uplift_dir": str(uplift_dir),
        "score_month": target_month.isoformat(),
        "measures": measures,
        "feature_columns": FEATURE_COLUMNS,
        "max_train_rows": max_train_rows,
        "random_state": random_state,
        "tables": table_metrics,
        "measures_trained": len(measure_summary),
        "recommendations_rows": len(recommendations_rows),
        "hybrid_lookup_used": bool(gate_lookup),
        "action_limits": {action: action_limits.get(action) for action in ACTION_TYPES},
        "action_usage": {action: int(limit_usage.get(action, 0)) for action in ACTION_TYPES},
        "action_remaining": {action: remaining_limits.get(action) for action in ACTION_TYPES},
    }

    metrics_path = uplift_dir / "load_metrics_uplift.json"
    with metrics_path.open("w", encoding="utf-8") as fh:
        json.dump(metrics, fh, ensure_ascii=False, indent=2)

    return metrics


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    uplift_cfg = PIPELINE_CONFIG["uplift"]
    metrics = run(
        input_dir=Path(OUTPUT_DIR),
        out_dir=Path(OUTPUT_DIR),
        score_month=PIPELINE_CONFIG.get("month"),
        measures_raw=uplift_cfg.get("measures"),
        max_train_rows=int(uplift_cfg["max_train_rows"]),
        random_state=int(uplift_cfg["random_state"]),
    )
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
