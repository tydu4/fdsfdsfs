from __future__ import annotations

import json
import random
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

from .common import (
    discover_input_layer,
    ensure_dir,
    maybe_write_parquet_from_csv,
    parse_as_of_month,
    parse_date_any,
    read_csv_dict,
    write_csv_rows,
)
from .settings import OUTPUT_DIR, PIPELINE_CONFIG


MART_DIRNAME = "mart"
SEGMENTATION_DIRNAME = "segmentation"
UPLIFT_DIRNAME = "uplift"

DEFAULT_MEASURES = (
    "sms",
    "operator_call",
    "restriction_notice",
)

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
    out = [x for x in parts if x]
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
                        "best_measure": "",
                        "best_uplift": -999.0,
                        "second_measure": "",
                        "second_uplift": -999.0,
                    }

                rec = recommendations[rec_key]
                u_val = float(u_i)
                if u_val > float(rec["best_uplift"]):
                    rec["second_measure"] = rec["best_measure"]
                    rec["second_uplift"] = rec["best_uplift"]
                    rec["best_measure"] = measure
                    rec["best_uplift"] = u_val
                elif u_val > float(rec["second_uplift"]):
                    rec["second_measure"] = measure
                    rec["second_uplift"] = u_val

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

    # Build recommendation table
    recommendations_rows: list[dict[str, object]] = []
    for _, rec in sorted(recommendations.items(), key=lambda x: (x[0][1], int(x[0][0]))):
        gate_ml_eligible = str(rec.get("gate_ml_eligible", "")).strip()
        eligible = gate_ml_eligible not in {"0", "false", "False"}

        best_measure = str(rec.get("best_measure", ""))
        best_uplift = float(rec.get("best_uplift", -999.0))
        second_measure = str(rec.get("second_measure", ""))
        second_uplift = float(rec.get("second_uplift", -999.0))

        if not eligible:
            recommendation_reason = "gate_ml_ineligible"
            recommended_measure = ""
            recommended_uplift = ""
        elif best_uplift <= 0:
            recommendation_reason = "non_positive_uplift"
            recommended_measure = ""
            recommended_uplift = round(best_uplift, 8)
        else:
            recommendation_reason = "max_uplift"
            recommended_measure = best_measure
            recommended_uplift = round(best_uplift, 8)

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
                "second_uplift": "" if second_uplift <= -900 else round(second_uplift, 8),
                "recommendation_reason": recommendation_reason,
            }
        )

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
