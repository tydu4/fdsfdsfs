from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from pipeline.build_presentation_notes import run as run_notes


class PresentationNotesTests(unittest.TestCase):
    def _write_csv(self, path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_notes_generation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            presentation = base / "presentation"
            presentation.mkdir(parents=True, exist_ok=True)

            seg = base / "segmentation"
            uplift = base / "uplift"
            seg.mkdir(parents=True, exist_ok=True)
            uplift.mkdir(parents=True, exist_ok=True)

            (seg / "load_metrics_segmentation.json").write_text(
                json.dumps(
                    {
                        "rows_segmented": 1000,
                        "target_month": "2026-02-01",
                        "hybrid_meta": {
                            "hybrid_ml_eligible_rows": 400,
                            "hybrid_ml_ineligible_rows": 600,
                        },
                    }
                ),
                encoding="utf-8",
            )
            (uplift / "load_metrics_uplift.json").write_text(
                json.dumps({"score_month": "2026-02-01"}),
                encoding="utf-8",
            )

            self._write_csv(
                seg / "segment_hybrid_gate_profile.csv",
                ["business_gate", "share_total_pct"],
                [
                    {"business_gate": "gate_standard_collect", "share_total_pct": 55.0},
                    {"business_gate": "gate_soft_monitor", "share_total_pct": 45.0},
                ],
            )

            self._write_csv(
                uplift / "uplift_measure_summary.csv",
                ["measure", "ate_dr"],
                [
                    {"measure": "sms", "ate_dr": -0.1},
                    {"measure": "operator_call", "ate_dr": 0.2},
                ],
            )

            self._write_csv(
                uplift / "uplift_recommendations.csv",
                ["recommended_measure", "recommendation_reason"],
                [
                    {"recommended_measure": "operator_call", "recommendation_reason": "max_uplift"},
                    {"recommended_measure": "", "recommendation_reason": "gate_ml_ineligible"},
                    {"recommended_measure": "operator_call", "recommendation_reason": "max_uplift"},
                ],
            )

            metrics = run_notes(base, presentation)
            self.assertEqual(metrics["stage"], "build_presentation_notes")

            self.assertTrue((presentation / "deck_outline.md").exists())
            self.assertTrue((presentation / "speaker_notes.md").exists())
            self.assertTrue((presentation / "presentation_checklist.md").exists())
            self.assertTrue((presentation / "load_metrics_presentation_notes.json").exists())


if __name__ == "__main__":
    unittest.main()
