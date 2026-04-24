from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.run_pipeline import run as run_pipeline


class RunPipelineTests(unittest.TestCase):
    def _write_balance(self, path: Path, months: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = ["ls_id", "period_month", "debt_start", "charge_amt", "paid_amt", "source_row_num"]
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for i, month in enumerate(months, start=1):
                writer.writerow(
                    {
                        "ls_id": "1",
                        "period_month": month,
                        "debt_start": "100",
                        "charge_amt": "50",
                        "paid_amt": "20",
                        "source_row_num": str(i),
                    }
                )

    def test_pipeline_auto_month_and_summary_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            input_dir = base / "input"
            out_dir = base / "out"
            input_dir.mkdir(parents=True, exist_ok=True)

            overrides = {
                "input_dir": str(input_dir),
                "out_dir": str(out_dir),
            }
            progress_events: list[dict[str, object]] = []

            def fake_prepare(raw_input: Path, raw_out: Path):
                self.assertEqual(raw_input, input_dir.resolve())
                self.assertEqual(raw_out, out_dir.resolve())
                (raw_out / "raw").mkdir(parents=True, exist_ok=True)
                return {"stage": "prepare_raw"}

            def fake_normalized(norm_input: Path, norm_out: Path):
                self.assertEqual(norm_input, out_dir.resolve())
                self.assertEqual(norm_out, out_dir.resolve())
                self._write_balance(
                    norm_out / "normalized" / "fact_balance_monthly.csv",
                    ["2026-01-01", "2026-02-01"],
                )
                return {"stage": "build_normalized"}

            def fake_mart(mart_input: Path, mart_out: Path, as_of_month: str):
                self.assertEqual(mart_input, out_dir.resolve())
                self.assertEqual(mart_out, out_dir.resolve())
                self.assertEqual(as_of_month, "2026-02")
                return {"stage": "build_mart", "as_of_month": as_of_month}

            def fake_segmentation(
                input_dir: Path,
                out_dir: Path,
                segment_month: str | None,
                n_clusters: int,
                random_state: int,
                engine: str,
                with_hybrid: bool,
                k_hybrid_per_gate: int,
            ):
                self.assertEqual(segment_month, "2026-02")
                self.assertEqual(n_clusters, 8)
                self.assertEqual(random_state, 42)
                self.assertEqual(engine, "auto")
                self.assertTrue(with_hybrid)
                self.assertEqual(k_hybrid_per_gate, 4)
                return {"stage": "build_segmentation"}

            def fake_uplift(
                input_dir: Path,
                out_dir: Path,
                score_month: str | None,
                measures_raw: str | None,
                max_train_rows: int,
                random_state: int,
            ):
                self.assertEqual(score_month, "2026-02")
                self.assertEqual(measures_raw, "sms,operator_call,restriction_notice")
                self.assertEqual(max_train_rows, 250000)
                self.assertEqual(random_state, 42)
                return {"stage": "build_uplift_prototype"}

            def fake_viz(input_dir: Path, out_dir: Path, score_month: str | None, dpi: int):
                self.assertEqual(score_month, "2026-02")
                self.assertEqual(dpi, 180)
                return {"stage": "build_presentation_viz"}

            def fake_notes(input_dir: Path, out_dir: Path):
                self.assertTrue(out_dir.exists())
                return {"stage": "build_presentation_notes"}

            with (
                patch("pipeline.run_pipeline.run_prepare_raw", side_effect=fake_prepare),
                patch("pipeline.run_pipeline.run_build_normalized", side_effect=fake_normalized),
                patch("pipeline.run_pipeline.run_build_mart", side_effect=fake_mart),
                patch("pipeline.run_pipeline.run_build_segmentation", side_effect=fake_segmentation),
                patch("pipeline.run_pipeline.run_build_uplift_prototype", side_effect=fake_uplift),
                patch("pipeline.run_pipeline.run_build_presentation_viz", side_effect=fake_viz),
                patch("pipeline.run_pipeline.run_build_presentation_notes", side_effect=fake_notes),
            ):
                metrics = run_pipeline(overrides=overrides, progress_callback=progress_events.append)

            self.assertEqual(metrics["month"], "2026-02")
            self.assertEqual(len(metrics["stages"]), 7)
            self.assertTrue(any(event.get("status") == "month_detected" for event in progress_events))

            summary_path = out_dir / "load_metrics_pipeline.json"
            self.assertTrue(summary_path.exists())
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["month"], "2026-02")

    def test_pipeline_uses_explicit_month_and_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            input_dir = base / "input"
            out_dir = base / "out"
            slides_dir = base / "slides"
            input_dir.mkdir(parents=True, exist_ok=True)

            overrides = {
                "input_dir": str(input_dir),
                "out_dir": str(out_dir),
                "month": "2025-12",
                "segmentation": {
                    "k": 3,
                    "random_state": 7,
                    "engine": "python",
                    "with_hybrid": False,
                    "k_hybrid_per_gate": 2,
                },
                "uplift": {
                    "measures": "sms,operator_call",
                    "max_train_rows": 1000,
                    "random_state": 9,
                },
                "presentation": {
                    "out_dir": str(slides_dir),
                    "dpi": 220,
                },
            }

            def fake_prepare(raw_input: Path, raw_out: Path):
                return {"stage": "prepare_raw"}

            def fake_normalized(norm_input: Path, norm_out: Path):
                return {"stage": "build_normalized"}

            def fake_mart(mart_input: Path, mart_out: Path, as_of_month: str):
                self.assertEqual(as_of_month, "2025-12")
                return {"stage": "build_mart", "as_of_month": as_of_month}

            def fake_segmentation(
                input_dir: Path,
                out_dir: Path,
                segment_month: str | None,
                n_clusters: int,
                random_state: int,
                engine: str,
                with_hybrid: bool,
                k_hybrid_per_gate: int,
            ):
                self.assertEqual(segment_month, "2025-12")
                self.assertEqual(n_clusters, 3)
                self.assertEqual(random_state, 7)
                self.assertEqual(engine, "python")
                self.assertFalse(with_hybrid)
                self.assertEqual(k_hybrid_per_gate, 2)
                return {"stage": "build_segmentation"}

            def fake_uplift(
                input_dir: Path,
                out_dir: Path,
                score_month: str | None,
                measures_raw: str | None,
                max_train_rows: int,
                random_state: int,
            ):
                self.assertEqual(score_month, "2025-12")
                self.assertEqual(measures_raw, "sms,operator_call")
                self.assertEqual(max_train_rows, 1000)
                self.assertEqual(random_state, 9)
                return {"stage": "build_uplift_prototype"}

            def fake_viz(input_dir: Path, out_dir: Path, score_month: str | None, dpi: int):
                self.assertEqual(out_dir, slides_dir.resolve())
                self.assertEqual(score_month, "2025-12")
                self.assertEqual(dpi, 220)
                return {"stage": "build_presentation_viz"}

            def fake_notes(input_dir: Path, out_dir: Path):
                self.assertEqual(out_dir, slides_dir.resolve())
                return {"stage": "build_presentation_notes"}

            with (
                patch("pipeline.run_pipeline.run_prepare_raw", side_effect=fake_prepare),
                patch("pipeline.run_pipeline.run_build_normalized", side_effect=fake_normalized),
                patch("pipeline.run_pipeline.run_build_mart", side_effect=fake_mart),
                patch("pipeline.run_pipeline.run_build_segmentation", side_effect=fake_segmentation),
                patch("pipeline.run_pipeline.run_build_uplift_prototype", side_effect=fake_uplift),
                patch("pipeline.run_pipeline.run_build_presentation_viz", side_effect=fake_viz),
                patch("pipeline.run_pipeline.run_build_presentation_notes", side_effect=fake_notes),
            ):
                metrics = run_pipeline(overrides=overrides)

            self.assertEqual(metrics["month"], "2025-12")


if __name__ == "__main__":
    unittest.main()
