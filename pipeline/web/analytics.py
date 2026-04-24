"""Analytics & aggregation: decision summaries, output collection, metrics."""
from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from ..settings import OUTPUT_DIR
from .helpers import (
    decision_rationale, file_signature, file_size_mb, gate_rationale,
    measure_constraints, pct, pretty_gate, pretty_measure, pretty_reason,
    read_csv_rows, read_json, to_float, to_int,
)


_DECISION_CACHE: dict[str, Any] = {"signature": None, "payload": None}


def _rows_from_metrics(output_dir: Path) -> dict[str, int]:
    rows: dict[str, int] = {}
    raw_metrics = read_json(output_dir / "raw" / "load_metrics_raw.json") or {}
    for item in raw_metrics.get("sources", []):
        output_name = item.get("raw_output")
        if output_name:
            rows[str(output_name)] = int(item.get("rows_written", 0))
    for metrics_path in output_dir.glob("*/load_metrics_*.json"):
        metrics = read_json(metrics_path) or {}
        for table in metrics.get("tables", []):
            csv_name = table.get("csv")
            if csv_name:
                rows[str(csv_name)] = int(table.get("rows", 0))
    return rows


def _collect_csv_outputs(output_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not output_dir.exists():
        return rows
    row_counts = _rows_from_metrics(output_dir)
    for path in sorted(output_dir.glob("*/*.csv")):
        rel = path.relative_to(output_dir).as_posix()
        rows.append({
            "name": path.name, "layer": path.parent.name,
            "rows": row_counts.get(path.name), "size_mb": file_size_mb(path),
            "url": f"/outputs/{rel}",
        })
    return rows


def collect_outputs(output_dir: Path = OUTPUT_DIR) -> dict[str, Any]:
    return {"tables": _collect_csv_outputs(output_dir)}


def collect_decision_summary(output_dir: Path = OUTPUT_DIR) -> dict[str, Any]:
    paths = [
        output_dir / "load_metrics_pipeline.json",
        output_dir / "segmentation" / "segment_hybrid_gate_profile.csv",
        output_dir / "segmentation" / "segment_hybrid_cluster_profile.csv",
        output_dir / "segmentation" / "segment_hybrid_assignments.csv",
        output_dir / "uplift" / "uplift_measure_summary.csv",
        output_dir / "uplift" / "uplift_recommendations.csv",
        output_dir / "uplift" / "load_metrics_uplift.json",
    ]
    signature = file_signature(paths)
    if _DECISION_CACHE["signature"] == signature:
        return _DECISION_CACHE["payload"]

    pipeline_metrics = read_json(output_dir / "load_metrics_pipeline.json") or {}
    uplift_metrics = read_json(output_dir / "uplift" / "load_metrics_uplift.json") or {}
    gate_rows = read_csv_rows(output_dir / "segmentation" / "segment_hybrid_gate_profile.csv")
    cluster_rows = read_csv_rows(output_dir / "segmentation" / "segment_hybrid_cluster_profile.csv")
    measure_rows = read_csv_rows(output_dir / "uplift" / "uplift_measure_summary.csv")
    recommendation_rows = read_csv_rows(output_dir / "uplift" / "uplift_recommendations.csv")
    assignment_rows = read_csv_rows(output_dir / "segmentation" / "segment_hybrid_assignments.csv")

    total_accounts = len(recommendation_rows)
    rec_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    positive_actions = 0
    positive_uplift_sum = 0.0

    profile_features: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for row in assignment_rows:
        profile = row.get("hybrid_segment", "") or row.get("business_gate", "")
        if not profile:
            continue
        item = profile_features[profile]
        item["count"] += 1
        item["debt"] += to_float(row.get("debt_start_t"))
        item["pay_3m"] += to_float(row.get("payment_amount_l3m"))
        item["actions_3m"] += to_float(row.get("action_total_l3m"))
        item["contacts"] += to_float(row.get("contact_channels_count"))

    profiles: dict[str, dict[str, Any]] = {}
    for row in recommendation_rows:
        profile = row.get("hybrid_segment", "") or row.get("business_gate", "") or "unknown_profile"
        gate = row.get("business_gate", "") or "unknown_gate"
        recommendation = (row.get("recommended_measure", "") or "").strip() or "no_action"
        reason = (row.get("recommendation_reason", "") or "").strip()
        uplift = to_float(row.get("recommended_uplift"), 0.0) if recommendation != "no_action" else 0.0

        rec_counts[recommendation] += 1
        reason_counts[reason] += 1
        if recommendation != "no_action" and uplift > 0:
            positive_actions += 1
            positive_uplift_sum += uplift

        item = profiles.setdefault(profile, {
            "profile": profile, "business_gate": gate, "accounts": 0,
            "recommendations": Counter(), "reasons": Counter(),
            "positive_uplift_sum": 0.0, "positive_uplift_count": 0,
        })
        item["accounts"] += 1
        item["recommendations"][recommendation] += 1
        item["reasons"][reason] += 1
        if recommendation != "no_action" and uplift > 0:
            item["positive_uplift_sum"] += uplift
            item["positive_uplift_count"] += 1

    profile_cards: list[dict[str, Any]] = []
    for profile, item in profiles.items():
        recommendation, recommendation_count = item["recommendations"].most_common(1)[0]
        reason, reason_count = item["reasons"].most_common(1)[0]
        features = profile_features.get(profile, {})
        count = max(float(features.get("count", item["accounts"])), 1.0)
        avg_debt = float(features.get("debt", 0.0)) / count
        avg_pay_3m = float(features.get("pay_3m", 0.0)) / count
        avg_actions_3m = float(features.get("actions_3m", 0.0)) / count
        avg_contacts = float(features.get("contacts", 0.0)) / count
        avg_uplift = item["positive_uplift_sum"] / max(float(item["positive_uplift_count"]), 1.0)
        rationale, economic_effect = decision_rationale(
            gate=item["business_gate"], recommendation=recommendation, reason=reason,
            avg_uplift=avg_uplift, incremental_payers=item["positive_uplift_sum"], avg_debt=avg_debt,
        )
        profile_cards.append({
            "profile": profile,
            "profile_label": profile.replace("gate_", "").replace("__rule_only", " rule-only").replace("_", " "),
            "business_gate": item["business_gate"],
            "business_gate_label": pretty_gate(item["business_gate"]),
            "accounts": int(item["accounts"]),
            "share_pct": pct(item["accounts"], total_accounts),
            "top_recommendation": recommendation,
            "top_recommendation_label": pretty_measure(recommendation),
            "top_recommendation_count": int(recommendation_count),
            "top_recommendation_share_pct": pct(recommendation_count, item["accounts"]),
            "dominant_reason": reason,
            "dominant_reason_label": pretty_reason(reason),
            "dominant_reason_count": int(reason_count),
            "avg_positive_uplift_pct": round(avg_uplift * 100.0, 2),
            "expected_incremental_payers": round(float(item["positive_uplift_sum"]), 1),
            "avg_debt": round(avg_debt, 2),
            "avg_payment_3m": round(avg_pay_3m, 2),
            "avg_actions_3m": round(avg_actions_3m, 2),
            "avg_contacts": round(avg_contacts, 2),
            "rationale": rationale,
            "economic_effect": economic_effect,
            "business_constraints": measure_constraints(recommendation),
        })
    profile_cards.sort(key=lambda x: x["accounts"], reverse=True)

    gates = []
    for row in gate_rows:
        gate = row.get("business_gate", "")
        gates.append({
            "business_gate": gate, "label": pretty_gate(gate),
            "accounts": to_int(row.get("ls_count")),
            "share_pct": to_float(row.get("share_total_pct")),
            "ml_eligible": to_int(row.get("ml_eligible_ls_count")),
            "ml_eligible_share_pct": to_float(row.get("ml_eligible_share_pct")),
            "reason": row.get("top_gate_reason", ""),
            "rationale": gate_rationale(gate),
        })
    gates.sort(key=lambda x: x["accounts"], reverse=True)

    model_quality = []
    for row in measure_rows:
        measure = row.get("measure", "")
        ate_dr = to_float(row.get("ate_dr"))
        verdict = "Положительный средний эффект" if ate_dr > 0 else "Средний эффект неположительный"
        model_quality.append({
            "measure": measure, "measure_label": pretty_measure(measure),
            "train_rows": to_int(row.get("train_rows")),
            "treated_rows": to_int(row.get("train_treated_rows")),
            "treated_share_pct": round(to_float(row.get("train_treated_share")) * 100.0, 2),
            "outcome_rate_pct": round(to_float(row.get("train_outcome_rate")) * 100.0, 2),
            "treated_outcome_pct": round(to_float(row.get("treated_outcome_rate")) * 100.0, 2),
            "control_outcome_pct": round(to_float(row.get("control_outcome_rate")) * 100.0, 2),
            "naive_ate_pct": round(to_float(row.get("naive_ate")) * 100.0, 2),
            "ate_dr_pct": round(ate_dr * 100.0, 2),
            "avg_pred_uplift_pct": round(to_float(row.get("avg_pred_uplift_train")) * 100.0, 2),
            "score_rows": to_int(row.get("score_rows")),
            "verdict": verdict, "constraints": measure_constraints(measure),
        })
    model_quality.sort(key=lambda x: x["ate_dr_pct"], reverse=True)

    rec_mix = [
        {"measure": m, "label": pretty_measure(m), "accounts": c,
         "share_pct": pct(c, total_accounts), "constraints": measure_constraints(m)}
        for m, c in rec_counts.most_common()
    ]

    cluster_mix_by_gate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in cluster_rows:
        gate = row.get("business_gate", "")
        cluster_mix_by_gate[gate].append({
            "cluster_id": to_int(row.get("hybrid_cluster_id")),
            "accounts": to_int(row.get("ls_count")),
            "share_within_gate_pct": to_float(row.get("share_within_gate_pct")),
        })

    payload = {
        "score_month": str(pipeline_metrics.get("month") or uplift_metrics.get("score_month") or "")[:7],
        "overview": {
            "total_accounts": total_accounts,
            "positive_actions": positive_actions,
            "positive_actions_share_pct": pct(positive_actions, total_accounts),
            "no_action": rec_counts.get("no_action", 0),
            "no_action_share_pct": pct(rec_counts.get("no_action", 0), total_accounts),
            "rule_blocked": reason_counts.get("gate_ml_ineligible", 0),
            "rule_blocked_share_pct": pct(reason_counts.get("gate_ml_ineligible", 0), total_accounts),
            "non_positive_uplift": reason_counts.get("non_positive_uplift", 0),
            "non_positive_uplift_share_pct": pct(reason_counts.get("non_positive_uplift", 0), total_accounts),
            "expected_incremental_payers": round(positive_uplift_sum, 1),
        },
        "gates": gates, "profiles": profile_cards, "profile_count": len(profile_cards),
        "recommendation_mix": rec_mix,
        "reason_mix": [
            {"reason": r, "label": pretty_reason(r), "accounts": c, "share_pct": pct(c, total_accounts)}
            for r, c in reason_counts.most_common()
        ],
        "model_quality": model_quality,
        "cluster_mix_by_gate": cluster_mix_by_gate,
    }
    _DECISION_CACHE["signature"] = signature
    _DECISION_CACHE["payload"] = payload
    return payload
