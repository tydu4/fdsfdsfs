from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from pipeline.build_presentation_viz import run as run_viz


class PresentationVizTests(unittest.TestCase):
    def _write_csv(self, path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_visualization_pack_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            normalized = base / "normalized"
            segmentation = base / "segmentation"
            uplift = base / "uplift"

            self._write_csv(
                normalized / "dq_issues.csv",
                ["issue_type", "issue_id"],
                [
                    {"issue_type": "NON_NUMERIC_LS", "issue_id": 1},
                    {"issue_type": "NON_NUMERIC_LS", "issue_id": 2},
                    {"issue_type": "CONFLICT_DUPLICATE_LS_SETUP", "issue_id": 3},
                ],
            )

            self._write_csv(
                segmentation / "segment_hybrid_gate_profile.csv",
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
                [
                    {
                        "business_gate": "gate_standard_collect",
                        "ls_count": 70,
                        "share_total_pct": 70.0,
                        "ml_eligible_ls_count": 70,
                        "ml_eligible_share_pct": 100.0,
                        "top_gate_reason": "default_collect_path",
                        "top_gate_reason_count": 70,
                        "gate_inertia": 100.0,
                        "gate_kmeans_engine": "sklearn_minibatch",
                    },
                    {
                        "business_gate": "gate_closed_no_debt",
                        "ls_count": 30,
                        "share_total_pct": 30.0,
                        "ml_eligible_ls_count": 0,
                        "ml_eligible_share_pct": 0.0,
                        "top_gate_reason": "debt_non_positive",
                        "top_gate_reason_count": 30,
                        "gate_inertia": 0.0,
                        "gate_kmeans_engine": "",
                    },
                ],
            )

            self._write_csv(
                segmentation / "segment_hybrid_cluster_profile.csv",
                [
                    "business_gate",
                    "hybrid_cluster_id",
                    "ls_count",
                    "share_within_gate_pct",
                    "k_effective_in_gate",
                    "k_requested_in_gate",
                    "kmeans_engine_in_gate",
                ],
                [
                    {
                        "business_gate": "gate_standard_collect",
                        "hybrid_cluster_id": 0,
                        "ls_count": 40,
                        "share_within_gate_pct": 57.1,
                        "k_effective_in_gate": 2,
                        "k_requested_in_gate": 2,
                        "kmeans_engine_in_gate": "sklearn_minibatch",
                    },
                    {
                        "business_gate": "gate_standard_collect",
                        "hybrid_cluster_id": 1,
                        "ls_count": 30,
                        "share_within_gate_pct": 42.9,
                        "k_effective_in_gate": 2,
                        "k_requested_in_gate": 2,
                        "kmeans_engine_in_gate": "sklearn_minibatch",
                    },
                ],
            )

            self._write_csv(
                uplift / "uplift_measure_summary.csv",
                [
                    "measure",
                    "naive_ate",
                    "ate_dr",
                ],
                [
                    {"measure": "sms", "naive_ate": -0.02, "ate_dr": 0.05},
                    {"measure": "operator_call", "naive_ate": 0.01, "ate_dr": 0.12},
                ],
            )

            self._write_csv(
                uplift / "uplift_recommendations.csv",
                [
                    "ls_id",
                    "month",
                    "recommended_measure",
                    "recommendation_reason",
                ],
                [
                    {"ls_id": 1, "month": "2026-02-01", "recommended_measure": "operator_call", "recommendation_reason": "max_uplift"},
                    {"ls_id": 2, "month": "2026-02-01", "recommended_measure": "", "recommendation_reason": "gate_ml_ineligible"},
                ],
            )

            self._write_csv(
                uplift / "uplift_measure_scores.csv",
                [
                    "ls_id",
                    "month",
                    "measure",
                    "uplift_score",
                    "gate_ml_eligible",
                ],
                [
                    {"ls_id": 1, "month": "2026-02-01", "measure": "sms", "uplift_score": 0.01, "gate_ml_eligible": 1},
                    {"ls_id": 1, "month": "2026-02-01", "measure": "operator_call", "uplift_score": 0.05, "gate_ml_eligible": 1},
                    {"ls_id": 2, "month": "2026-02-01", "measure": "sms", "uplift_score": -0.01, "gate_ml_eligible": 0},
                ],
            )

            out_dir = base / "presentation"
            metrics = run_viz(base, out_dir, "2026-02", 180)

            self.assertGreater(metrics["charts_count"], 0)
            self.assertTrue((out_dir / "presentation_storyline.md").exists())
            self.assertTrue((out_dir / "load_metrics_presentation_viz.json").exists())


if __name__ == "__main__":
    unittest.main()
