from __future__ import annotations

import json
import math
import random
import sys
from collections import defaultdict
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


KMEANS_FEATURES = [
    "debt_start_t",
    "charge_amt_t",
    "paid_amt_t",
    "debt_change_1m",
    "payment_txn_count_l3m",
    "payment_amount_l3m",
    "action_total_l3m",
    "action_unique_types_l3m",
    "contact_channels_count",
    "has_phone",
    "has_e_receipt",
    "can_remote_disconnect",
]


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


def _score_rule(row: dict[str, str]) -> tuple[int, str, str]:
    debt = _to_float(row.get("debt_start_t", ""))
    debt_change = _to_float(row.get("debt_change_1m", ""))
    pay_cnt_3m = _to_int(row.get("payment_txn_count_l3m", ""))
    pay_amt_3m = _to_float(row.get("payment_amount_l3m", ""))
    action_total_3m = _to_int(row.get("action_total_l3m", ""))
    contact_channels = _to_int(row.get("contact_channels_count", ""))
    has_phone = _to_int(row.get("has_phone", ""))
    has_e_receipt = _to_int(row.get("has_e_receipt", ""))

    score = 0
    reasons: list[str] = []

    if debt >= 30000:
        score += 4
        reasons.append("debt_ge_30k")
    elif debt >= 10000:
        score += 3
        reasons.append("debt_ge_10k")
    elif debt >= 2000:
        score += 2
        reasons.append("debt_ge_2k")
    elif debt > 0:
        score += 1
        reasons.append("debt_gt_0")

    if debt_change > 0:
        score += 2
        reasons.append("debt_growing")

    if pay_cnt_3m == 0:
        score += 3
        reasons.append("no_pay_txn_3m")
    elif pay_cnt_3m == 1:
        score += 1
        reasons.append("single_pay_txn_3m")

    if pay_amt_3m <= 0:
        score += 2
        reasons.append("no_positive_pay_amount_3m")

    if contact_channels == 0:
        score += 1
        reasons.append("no_contact_channels")

    if action_total_3m >= 5:
        score += 1
        reasons.append("many_actions_3m")

    if has_phone == 0 and has_e_receipt == 0:
        score += 1
        reasons.append("no_digital_contact")

    if score >= 10:
        segment = "rule_critical"
    elif score >= 7:
        segment = "rule_high"
    elif score >= 4:
        segment = "rule_medium"
    else:
        segment = "rule_low"

    return score, segment, ";".join(reasons)


def _business_gate(row: dict[str, str]) -> tuple[str, str, bool]:
    debt = _to_float(row.get("debt_start_t", ""))
    pay_cnt_3m = _to_int(row.get("payment_txn_count_l3m", ""))
    pay_amt_3m = _to_float(row.get("payment_amount_l3m", ""))
    contact_channels = _to_int(row.get("contact_channels_count", ""))
    can_remote_disconnect = _to_int(row.get("can_remote_disconnect", ""))

    # Hard business gates first: these are not sent to ML clustering.
    if debt <= 0:
        return "gate_closed_no_debt", "debt_non_positive", False
    if debt < 1000 and pay_cnt_3m >= 2 and pay_amt_3m > 0:
        return "gate_soft_monitor", "small_debt_with_recent_payments", False

    # Priority / admissibility gates where ML is still useful inside each gate.
    if debt >= 30000 and pay_cnt_3m == 0 and pay_amt_3m <= 0:
        return "gate_prelegal_priority", "high_debt_no_recent_payments", True
    if contact_channels == 0:
        return "gate_contact_deficit", "no_contact_channels", True
    if can_remote_disconnect == 1:
        return "gate_restriction_eligible", "remote_disconnect_possible", True
    return "gate_standard_collect", "default_collect_path", True


def _standardize_matrix(matrix: list[list[float]]) -> tuple[list[list[float]], list[float], list[float]]:
    if not matrix:
        return [], [], []

    n = len(matrix)
    d = len(matrix[0])
    means = [0.0] * d
    stds = [1.0] * d

    for row in matrix:
        for j in range(d):
            means[j] += row[j]
    means = [x / n for x in means]

    for row in matrix:
        for j in range(d):
            delta = row[j] - means[j]
            stds[j] += delta * delta
    stds = [math.sqrt(max(v / n, 1e-12)) for v in stds]

    scaled: list[list[float]] = []
    for row in matrix:
        scaled.append([(row[j] - means[j]) / stds[j] for j in range(d)])
    return scaled, means, stds


def _dist2(a: list[float], b: list[float]) -> float:
    return sum((x - y) ** 2 for x, y in zip(a, b))


def _cluster_python_kmeans(
    matrix: list[list[float]],
    n_clusters: int,
    random_state: int,
    max_iter: int = 50,
) -> tuple[list[int], list[list[float]], float, int]:
    n = len(matrix)
    d = len(matrix[0])
    k = max(1, min(n_clusters, n))

    rng = random.Random(random_state)
    init_indices = rng.sample(range(n), k)
    centers = [matrix[idx][:] for idx in init_indices]
    labels = [0] * n

    for _ in range(max_iter):
        changed = 0

        for i, row in enumerate(matrix):
            best = 0
            best_dist = _dist2(row, centers[0])
            for c in range(1, k):
                cur_dist = _dist2(row, centers[c])
                if cur_dist < best_dist:
                    best = c
                    best_dist = cur_dist
            if labels[i] != best:
                labels[i] = best
                changed += 1

        sums = [[0.0] * d for _ in range(k)]
        counts = [0] * k
        for lbl, row in zip(labels, matrix):
            counts[lbl] += 1
            for j in range(d):
                sums[lbl][j] += row[j]

        for c in range(k):
            if counts[c] == 0:
                centers[c] = matrix[rng.randrange(n)][:]
                continue
            centers[c] = [sums[c][j] / counts[c] for j in range(d)]

        if changed == 0:
            break

    inertia = 0.0
    for lbl, row in zip(labels, matrix):
        inertia += _dist2(row, centers[lbl])

    return labels, centers, inertia, k


def _cluster_sklearn_minibatch(
    matrix: list[list[float]],
    n_clusters: int,
    random_state: int,
) -> tuple[list[int], list[list[float]], float, int]:
    import numpy as np  # type: ignore
    from sklearn.cluster import MiniBatchKMeans  # type: ignore

    n = len(matrix)
    k = max(1, min(n_clusters, n))
    arr = np.asarray(matrix, dtype=float)
    batch_size = max(512, min(8192, n // 10 if n > 0 else 512))

    model = MiniBatchKMeans(
        n_clusters=k,
        random_state=random_state,
        n_init=10,
        batch_size=batch_size,
        max_iter=200,
    )

    labels = model.fit_predict(arr)
    centers = model.cluster_centers_.tolist()
    inertia = float(model.inertia_)
    return labels.tolist(), centers, inertia, k


def _fit_kmeans(
    matrix: list[list[float]],
    n_clusters: int,
    random_state: int,
    engine: str,
) -> tuple[list[int], list[list[float]], float, int, str]:
    if engine not in {"auto", "sklearn", "python"}:
        raise ValueError("engine must be one of: auto, sklearn, python")

    if engine in {"auto", "sklearn"}:
        try:
            labels, centers, inertia, k = _cluster_sklearn_minibatch(matrix, n_clusters, random_state)
            return labels, centers, inertia, k, "sklearn_minibatch"
        except Exception:
            if engine == "sklearn":
                raise

    labels, centers, inertia, k = _cluster_python_kmeans(matrix, n_clusters, random_state)
    return labels, centers, inertia, k, "python_lloyd"


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


def _build_hybrid_outputs(
    assignments: list[dict[str, object]],
    matrix_raw: list[list[float]],
    n_clusters: int,
    k_hybrid_per_gate: int,
    random_state: int,
    engine: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    hybrid_assignments: list[dict[str, object]] = []
    gate_to_indices: dict[str, list[int]] = defaultdict(list)
    gate_reason_count: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for idx, (row, vector) in enumerate(zip(assignments, matrix_raw)):
        gate, gate_reason, ml_eligible = _business_gate(
            {
                "debt_start_t": str(row.get("debt_start_t", "")),
                "payment_txn_count_l3m": str(row.get("payment_txn_count_l3m", "")),
                "payment_amount_l3m": str(row.get("payment_amount_l3m", "")),
                "contact_channels_count": str(row.get("contact_channels_count", "")),
                "can_remote_disconnect": str(row.get("can_remote_disconnect", "")),
            }
        )

        out = dict(row)
        out["business_gate"] = gate
        out["gate_reason"] = gate_reason
        out["gate_ml_eligible"] = 1 if ml_eligible else 0
        out["hybrid_cluster_id"] = -1
        out["hybrid_segment"] = f"{gate}__rule_only"
        hybrid_assignments.append(out)

        gate_reason_count[gate][gate_reason] += 1
        if ml_eligible:
            gate_to_indices[gate].append(idx)

    cluster_meta: list[dict[str, object]] = []
    gate_inertia: dict[str, float] = defaultdict(float)
    gate_engine: dict[str, str] = {}

    for gate_idx, gate in enumerate(sorted(gate_to_indices.keys())):
        idxs = gate_to_indices[gate]
        if not idxs:
            continue

        gate_matrix_raw = [matrix_raw[i] for i in idxs]
        gate_matrix_scaled, _, _ = _standardize_matrix(gate_matrix_raw)
        requested_k = min(max(1, k_hybrid_per_gate), max(1, n_clusters), len(gate_matrix_scaled))
        labels, _, inertia, k_eff, engine_used = _fit_kmeans(
            gate_matrix_scaled,
            requested_k,
            random_state + (gate_idx + 1) * 17,
            engine,
        )
        gate_inertia[gate] = inertia
        gate_engine[gate] = engine_used

        for local_row_pos, label in enumerate(labels):
            global_idx = idxs[local_row_pos]
            hybrid_assignments[global_idx]["hybrid_cluster_id"] = int(label)
            hybrid_assignments[global_idx]["hybrid_segment"] = f"{gate}_c{label}"

        counts_by_cluster: dict[int, int] = defaultdict(int)
        for label in labels:
            counts_by_cluster[int(label)] += 1

        for cluster_id in sorted(counts_by_cluster.keys()):
            cnt = counts_by_cluster[cluster_id]
            cluster_meta.append(
                {
                    "business_gate": gate,
                    "hybrid_cluster_id": cluster_id,
                    "ls_count": cnt,
                    "share_within_gate_pct": round(100.0 * cnt / len(idxs), 4),
                    "k_effective_in_gate": k_eff,
                    "k_requested_in_gate": requested_k,
                    "kmeans_engine_in_gate": engine_used,
                }
            )

    total = max(len(hybrid_assignments), 1)
    gate_profile: list[dict[str, object]] = []
    gate_totals: dict[str, int] = defaultdict(int)
    gate_ml_eligible: dict[str, int] = defaultdict(int)

    for row in hybrid_assignments:
        gate = str(row["business_gate"])
        gate_totals[gate] += 1
        gate_ml_eligible[gate] += int(row["gate_ml_eligible"])

    for gate in sorted(gate_totals.keys()):
        reasons = gate_reason_count.get(gate, {})
        top_reason = ""
        top_reason_count = 0
        if reasons:
            top_reason, top_reason_count = max(reasons.items(), key=lambda x: x[1])

        gate_profile.append(
            {
                "business_gate": gate,
                "ls_count": gate_totals[gate],
                "share_total_pct": round(100.0 * gate_totals[gate] / total, 4),
                "ml_eligible_ls_count": gate_ml_eligible[gate],
                "ml_eligible_share_pct": round(100.0 * gate_ml_eligible[gate] / gate_totals[gate], 4),
                "top_gate_reason": top_reason,
                "top_gate_reason_count": top_reason_count,
                "gate_inertia": round(gate_inertia.get(gate, 0.0), 6),
                "gate_kmeans_engine": gate_engine.get(gate, ""),
            }
        )

    hybrid_meta = {
        "hybrid_gates_total": len(gate_totals),
        "hybrid_ml_eligible_rows": sum(gate_ml_eligible.values()),
        "hybrid_ml_ineligible_rows": len(hybrid_assignments) - sum(gate_ml_eligible.values()),
        "hybrid_gate_inertia": {k: gate_inertia[k] for k in sorted(gate_inertia.keys())},
        "hybrid_gate_engine": {k: gate_engine[k] for k in sorted(gate_engine.keys())},
    }
    return hybrid_assignments, gate_profile, cluster_meta, hybrid_meta


def run(
    input_dir: Path,
    out_dir: Path,
    segment_month: str | None,
    n_clusters: int,
    random_state: int,
    engine: str,
    with_hybrid: bool = False,
    k_hybrid_per_gate: int = 4,
) -> dict[str, object]:
    mart_dir = discover_input_layer(input_dir, MART_DIRNAME)
    mart_csv_path = mart_dir / "mart_ls_month.csv"
    if not mart_csv_path.exists():
        raise FileNotFoundError(f"File not found: {mart_csv_path}")

    segmentation_dir = out_dir / SEGMENTATION_DIRNAME
    ensure_dir(segmentation_dir)

    if segment_month:
        target_month = parse_as_of_month(segment_month)
    else:
        target_month = _detect_latest_month(mart_csv_path)

    selected_rows: list[dict[str, str]] = []
    for row in read_csv_dict(mart_csv_path):
        month = parse_date_any(row.get("month", ""))
        if month is None or month != target_month:
            continue
        selected_rows.append(row)

    if not selected_rows:
        raise ValueError(f"No rows found for month {target_month.isoformat()}")

    assignments: list[dict[str, object]] = []
    matrix_raw: list[list[float]] = []

    for row in selected_rows:
        rule_score, rule_segment, rule_reason = _score_rule(row)
        vector = [_to_float(row.get(feature, "")) for feature in KMEANS_FEATURES]
        matrix_raw.append(vector)
        assignments.append(
            {
                "ls_id": row.get("ls_id", ""),
                "month": row.get("month", ""),
                "rule_score": rule_score,
                "rule_segment": rule_segment,
                "rule_reason": rule_reason,
                "debt_start_t": row.get("debt_start_t", ""),
                "payment_txn_count_l3m": row.get("payment_txn_count_l3m", ""),
                "payment_amount_l3m": row.get("payment_amount_l3m", ""),
                "action_total_l3m": row.get("action_total_l3m", ""),
                "contact_channels_count": row.get("contact_channels_count", ""),
                "can_remote_disconnect": row.get("can_remote_disconnect", ""),
            }
        )

    matrix_scaled, means, stds = _standardize_matrix(matrix_raw)
    labels, centers, inertia, k_eff, engine_used = _fit_kmeans(matrix_scaled, n_clusters, random_state, engine)

    for out_row, label in zip(assignments, labels):
        rule_segment = str(out_row["rule_segment"])
        out_row["kmeans_cluster_id"] = label
        out_row["baseline_segment"] = f"{rule_segment}_c{label}"

    cluster_agg: dict[int, dict[str, float]] = defaultdict(
        lambda: {
            "count": 0.0,
            "sum_rule_score": 0.0,
            "sum_debt": 0.0,
            "sum_pay_amt_3m": 0.0,
            "sum_actions_3m": 0.0,
            "sum_contacts": 0.0,
            "rule_critical_count": 0.0,
            "rule_high_count": 0.0,
        }
    )

    total_count = float(len(assignments))
    for row in assignments:
        cluster_id = int(row["kmeans_cluster_id"])
        agg = cluster_agg[cluster_id]
        agg["count"] += 1.0
        agg["sum_rule_score"] += float(row["rule_score"])
        agg["sum_debt"] += _to_float(str(row.get("debt_start_t", "")))
        agg["sum_pay_amt_3m"] += _to_float(str(row.get("payment_amount_l3m", "")))
        agg["sum_actions_3m"] += _to_float(str(row.get("action_total_l3m", "")))
        agg["sum_contacts"] += _to_float(str(row.get("contact_channels_count", "")))
        if row.get("rule_segment") == "rule_critical":
            agg["rule_critical_count"] += 1.0
        if row.get("rule_segment") == "rule_high":
            agg["rule_high_count"] += 1.0

    cluster_profiles: list[dict[str, object]] = []
    for cluster_id in sorted(cluster_agg.keys()):
        agg = cluster_agg[cluster_id]
        cnt = max(agg["count"], 1.0)
        cluster_profiles.append(
            {
                "cluster_id": cluster_id,
                "ls_count": int(agg["count"]),
                "share_pct": round(100.0 * agg["count"] / total_count, 4),
                "avg_rule_score": round(agg["sum_rule_score"] / cnt, 4),
                "avg_debt_start_t": round(agg["sum_debt"] / cnt, 4),
                "avg_payment_amount_l3m": round(agg["sum_pay_amt_3m"] / cnt, 4),
                "avg_action_total_l3m": round(agg["sum_actions_3m"] / cnt, 4),
                "avg_contact_channels_count": round(agg["sum_contacts"] / cnt, 4),
                "rule_critical_share_pct": round(100.0 * agg["rule_critical_count"] / cnt, 4),
                "rule_high_share_pct": round(100.0 * agg["rule_high_count"] / cnt, 4),
            }
        )

    table_metrics: list[dict[str, object]] = []
    table_metrics.append(
        _write_table(
            segmentation_dir,
            "segment_baseline_assignments",
            [
                "ls_id",
                "month",
                "rule_score",
                "rule_segment",
                "rule_reason",
                "kmeans_cluster_id",
                "baseline_segment",
                "debt_start_t",
                "payment_txn_count_l3m",
                "payment_amount_l3m",
                "action_total_l3m",
                "contact_channels_count",
                "can_remote_disconnect",
            ],
            assignments,
        )
    )

    table_metrics.append(
        _write_table(
            segmentation_dir,
            "segment_baseline_cluster_profile",
            [
                "cluster_id",
                "ls_count",
                "share_pct",
                "avg_rule_score",
                "avg_debt_start_t",
                "avg_payment_amount_l3m",
                "avg_action_total_l3m",
                "avg_contact_channels_count",
                "rule_critical_share_pct",
                "rule_high_share_pct",
            ],
            cluster_profiles,
        )
    )

    hybrid_meta: dict[str, object] = {}
    if with_hybrid:
        hybrid_assignments, gate_profile, hybrid_cluster_profile, hybrid_meta = _build_hybrid_outputs(
            assignments=assignments,
            matrix_raw=matrix_raw,
            n_clusters=n_clusters,
            k_hybrid_per_gate=k_hybrid_per_gate,
            random_state=random_state,
            engine=engine,
        )

        table_metrics.append(
            _write_table(
                segmentation_dir,
                "segment_hybrid_assignments",
                [
                    "ls_id",
                    "month",
                    "rule_score",
                    "rule_segment",
                    "rule_reason",
                    "baseline_segment",
                    "business_gate",
                    "gate_reason",
                    "gate_ml_eligible",
                    "hybrid_cluster_id",
                    "hybrid_segment",
                    "debt_start_t",
                    "payment_txn_count_l3m",
                    "payment_amount_l3m",
                    "action_total_l3m",
                    "contact_channels_count",
                    "can_remote_disconnect",
                ],
                hybrid_assignments,
            )
        )

        table_metrics.append(
            _write_table(
                segmentation_dir,
                "segment_hybrid_gate_profile",
                [
                    "business_gate",
                    "ls_count",
                    "share_total_pct",
                    "ml_eligible_ls_count",
                    "ml_eligible_share_pct",
                    "top_gate_reason",
                    "top_gate_reason_count",
                    "gate_inertia",
                    "gate_kmeans_engine",
                ],
                gate_profile,
            )
        )

        table_metrics.append(
            _write_table(
                segmentation_dir,
                "segment_hybrid_cluster_profile",
                [
                    "business_gate",
                    "hybrid_cluster_id",
                    "ls_count",
                    "share_within_gate_pct",
                    "k_effective_in_gate",
                    "k_requested_in_gate",
                    "kmeans_engine_in_gate",
                ],
                hybrid_cluster_profile,
            )
        )

    metrics: dict[str, object] = {
        "stage": "build_segmentation_baseline",
        "input_mart_dir": str(mart_dir),
        "segmentation_dir": str(segmentation_dir),
        "target_month": target_month.isoformat(),
        "rows_segmented": len(assignments),
        "k_requested": n_clusters,
        "k_effective": k_eff,
        "kmeans_engine": engine_used,
        "kmeans_inertia": inertia,
        "kmeans_features": KMEANS_FEATURES,
        "feature_means": means,
        "feature_stds": stds,
        "with_hybrid": with_hybrid,
        "k_hybrid_per_gate": k_hybrid_per_gate,
        "hybrid_meta": hybrid_meta,
        "tables": table_metrics,
    }

    metrics_path = segmentation_dir / "load_metrics_segmentation.json"
    with metrics_path.open("w", encoding="utf-8") as fh:
        json.dump(metrics, fh, ensure_ascii=False, indent=2)

    return metrics


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    seg_cfg = PIPELINE_CONFIG["segmentation"]
    metrics = run(
        input_dir=Path(OUTPUT_DIR),
        out_dir=Path(OUTPUT_DIR),
        segment_month=PIPELINE_CONFIG.get("month"),
        n_clusters=int(seg_cfg["k"]),
        random_state=int(seg_cfg["random_state"]),
        engine=str(seg_cfg["engine"]),
        with_hybrid=bool(seg_cfg["with_hybrid"]),
        k_hybrid_per_gate=int(seg_cfg["k_hybrid_per_gate"]),
    )
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
