from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from pipeline.build_segmentation import run as run_build_segmentation
from pipeline.build_segmentation import _business_gate
from pipeline.build_segmentation import _score_rule


class RuleScorecardTests(unittest.TestCase):
    def test_rule_score_critical(self) -> None:
        row = {
            "debt_start_t": "40000",
            "debt_change_1m": "500",
            "payment_txn_count_l3m": "0",
            "payment_amount_l3m": "0",
            "action_total_l3m": "6",
            "contact_channels_count": "0",
            "has_phone": "0",
            "has_e_receipt": "0",
        }
        score, segment, reason = _score_rule(row)
        self.assertEqual(segment, "rule_critical")
        self.assertGreaterEqual(score, 10)
        self.assertIn("debt_ge_30k", reason)

    def test_rule_score_low(self) -> None:
        row = {
            "debt_start_t": "0",
            "debt_change_1m": "-50",
            "payment_txn_count_l3m": "4",
            "payment_amount_l3m": "2000",
            "action_total_l3m": "0",
            "contact_channels_count": "3",
            "has_phone": "1",
            "has_e_receipt": "1",
        }
        score, segment, _ = _score_rule(row)
        self.assertEqual(segment, "rule_low")
        self.assertEqual(score, 0)

    def test_business_gate_soft_monitor(self) -> None:
        gate, reason, eligible = _business_gate(
            {
                "debt_start_t": "500",
                "payment_txn_count_l3m": "3",
                "payment_amount_l3m": "1200",
                "contact_channels_count": "2",
                "can_remote_disconnect": "0",
            }
        )
        self.assertEqual(gate, "gate_soft_monitor")
        self.assertEqual(reason, "small_debt_with_recent_payments")
        self.assertFalse(eligible)

    def test_business_gate_prelegal_priority(self) -> None:
        gate, reason, eligible = _business_gate(
            {
                "debt_start_t": "45000",
                "payment_txn_count_l3m": "0",
                "payment_amount_l3m": "0",
                "contact_channels_count": "1",
                "can_remote_disconnect": "1",
            }
        )
        self.assertEqual(gate, "gate_prelegal_priority")
        self.assertEqual(reason, "high_debt_no_recent_payments")
        self.assertTrue(eligible)


class SegmentationRunTests(unittest.TestCase):
    def _write_mart(self, path: Path) -> None:
        fieldnames = [
            "ls_id",
            "month",
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
        rows = [
            {
                "ls_id": "1",
                "month": "2025-02-01",
                "debt_start_t": "100",
                "charge_amt_t": "10",
                "paid_amt_t": "9",
                "debt_change_1m": "-5",
                "payment_txn_count_l3m": "3",
                "payment_amount_l3m": "200",
                "action_total_l3m": "0",
                "action_unique_types_l3m": "0",
                "contact_channels_count": "3",
                "has_phone": "1",
                "has_e_receipt": "1",
                "can_remote_disconnect": "0",
            },
            {
                "ls_id": "2",
                "month": "2025-02-01",
                "debt_start_t": "120",
                "charge_amt_t": "15",
                "paid_amt_t": "10",
                "debt_change_1m": "-3",
                "payment_txn_count_l3m": "2",
                "payment_amount_l3m": "180",
                "action_total_l3m": "0",
                "action_unique_types_l3m": "0",
                "contact_channels_count": "2",
                "has_phone": "1",
                "has_e_receipt": "1",
                "can_remote_disconnect": "0",
            },
            {
                "ls_id": "3",
                "month": "2025-02-01",
                "debt_start_t": "35000",
                "charge_amt_t": "2000",
                "paid_amt_t": "0",
                "debt_change_1m": "2000",
                "payment_txn_count_l3m": "0",
                "payment_amount_l3m": "0",
                "action_total_l3m": "8",
                "action_unique_types_l3m": "4",
                "contact_channels_count": "0",
                "has_phone": "0",
                "has_e_receipt": "0",
                "can_remote_disconnect": "1",
            },
            {
                "ls_id": "4",
                "month": "2025-02-01",
                "debt_start_t": "28000",
                "charge_amt_t": "1800",
                "paid_amt_t": "10",
                "debt_change_1m": "1500",
                "payment_txn_count_l3m": "0",
                "payment_amount_l3m": "0",
                "action_total_l3m": "6",
                "action_unique_types_l3m": "3",
                "contact_channels_count": "0",
                "has_phone": "0",
                "has_e_receipt": "0",
                "can_remote_disconnect": "1",
            },
        ]

        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_segmentation_builds_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            mart_dir = base / "mart"
            mart_dir.mkdir(parents=True, exist_ok=True)
            self._write_mart(mart_dir / "mart_ls_month.csv")

            metrics = run_build_segmentation(
                input_dir=base,
                out_dir=base,
                segment_month="2025-02",
                n_clusters=2,
                random_state=7,
                engine="python",
            )

            self.assertEqual(metrics["k_effective"], 2)
            self.assertEqual(metrics["rows_segmented"], 4)

            assignments_path = base / "segmentation" / "segment_baseline_assignments.csv"
            clusters_path = base / "segmentation" / "segment_baseline_cluster_profile.csv"
            self.assertTrue(assignments_path.exists())
            self.assertTrue(clusters_path.exists())

            with assignments_path.open("r", encoding="utf-8", newline="") as fh:
                rows = list(csv.DictReader(fh))
            self.assertEqual(len(rows), 4)
            self.assertEqual(len({row["kmeans_cluster_id"] for row in rows}), 2)

    def test_segmentation_hybrid_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            mart_dir = base / "mart"
            mart_dir.mkdir(parents=True, exist_ok=True)
            self._write_mart(mart_dir / "mart_ls_month.csv")

            metrics = run_build_segmentation(
                input_dir=base,
                out_dir=base,
                segment_month="2025-02",
                n_clusters=3,
                random_state=11,
                engine="python",
                with_hybrid=True,
                k_hybrid_per_gate=2,
            )

            self.assertTrue(metrics["with_hybrid"])
            self.assertIn("hybrid_meta", metrics)

            hybrid_assignments_path = base / "segmentation" / "segment_hybrid_assignments.csv"
            hybrid_gate_profile_path = base / "segmentation" / "segment_hybrid_gate_profile.csv"
            hybrid_cluster_profile_path = base / "segmentation" / "segment_hybrid_cluster_profile.csv"
            self.assertTrue(hybrid_assignments_path.exists())
            self.assertTrue(hybrid_gate_profile_path.exists())
            self.assertTrue(hybrid_cluster_profile_path.exists())

            with hybrid_assignments_path.open("r", encoding="utf-8", newline="") as fh:
                rows = list(csv.DictReader(fh))
            self.assertEqual(len(rows), 4)
            self.assertTrue(all("business_gate" in row for row in rows))
            self.assertTrue(any(row["gate_ml_eligible"] == "0" for row in rows))


if __name__ == "__main__":
    unittest.main()
