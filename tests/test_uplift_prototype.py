from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from pipeline.build_uplift_prototype import FEATURE_COLUMNS
from pipeline.build_uplift_prototype import run as run_uplift


class UpliftPrototypeTests(unittest.TestCase):
    def _write_mart(self, path: Path) -> None:
        extra_actions = ["action_sms_l1m", "action_operator_call_l1m", "action_restriction_notice_l1m"]
        fieldnames = ["ls_id", "month", "paid_any_next_month"] + FEATURE_COLUMNS + extra_actions

        rows: list[dict[str, str]] = []

        # Training month
        for i in range(1, 31):
            sms_t = 1 if i % 3 == 0 else 0
            op_t = 1 if i % 4 == 0 else 0
            y = 1 if ((sms_t == 1 and i % 2 == 0) or (op_t == 1 and i % 5 != 0)) else 0

            rows.append(
                {
                    "ls_id": str(i),
                    "month": "2025-01-01",
                    "paid_any_next_month": str(y),
                    "debt_start_t": str(500 * i),
                    "charge_amt_t": str(200 + i),
                    "paid_amt_t": str(150 if y else 20),
                    "debt_change_1m": str(-30 if y else 120),
                    "payment_txn_count_l1m": str(2 if y else 0),
                    "payment_txn_count_l3m": str(3 if y else 1),
                    "payment_txn_count_l6m": str(5 if y else 2),
                    "payment_amount_l1m": str(200 if y else 0),
                    "payment_amount_l3m": str(600 if y else 80),
                    "payment_amount_l6m": str(1200 if y else 180),
                    "action_total_l1m": str(sms_t + op_t),
                    "action_total_l3m": str(2 + sms_t + op_t),
                    "action_total_l6m": str(4 + sms_t + op_t),
                    "action_unique_types_l1m": str(1 if (sms_t + op_t) > 0 else 0),
                    "action_unique_types_l3m": str(2 if (sms_t + op_t) > 0 else 1),
                    "action_unique_types_l6m": str(3 if (sms_t + op_t) > 0 else 1),
                    "contact_channels_count": str(2 if i % 6 != 0 else 0),
                    "has_phone": str(1 if i % 6 != 0 else 0),
                    "has_e_receipt": str(1 if i % 7 != 0 else 0),
                    "can_remote_disconnect": str(1 if i % 8 == 0 else 0),
                    "has_benefits": str(1 if i % 9 == 0 else 0),
                    "is_city": str(1 if i % 5 != 0 else 0),
                    "is_mkd": str(1 if i % 4 != 0 else 0),
                    "action_sms_l1m": str(sms_t),
                    "action_operator_call_l1m": str(op_t),
                    "action_restriction_notice_l1m": "0",
                }
            )

        # Score month
        for i in range(101, 106):
            rows.append(
                {
                    "ls_id": str(i),
                    "month": "2025-02-01",
                    "paid_any_next_month": "",
                    "debt_start_t": str(700 * (i - 100)),
                    "charge_amt_t": str(210 + i),
                    "paid_amt_t": "0",
                    "debt_change_1m": "100",
                    "payment_txn_count_l1m": "0",
                    "payment_txn_count_l3m": "1",
                    "payment_txn_count_l6m": "2",
                    "payment_amount_l1m": "0",
                    "payment_amount_l3m": "70",
                    "payment_amount_l6m": "160",
                    "action_total_l1m": "0",
                    "action_total_l3m": "1",
                    "action_total_l6m": "2",
                    "action_unique_types_l1m": "0",
                    "action_unique_types_l3m": "1",
                    "action_unique_types_l6m": "1",
                    "contact_channels_count": "1",
                    "has_phone": "1",
                    "has_e_receipt": "0",
                    "can_remote_disconnect": "0",
                    "has_benefits": "0",
                    "is_city": "1",
                    "is_mkd": "1",
                    "action_sms_l1m": "0",
                    "action_operator_call_l1m": "0",
                    "action_restriction_notice_l1m": "0",
                }
            )

        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def _write_hybrid_lookup(self, path: Path) -> None:
        fieldnames = ["ls_id", "month", "business_gate", "gate_ml_eligible", "hybrid_segment"]
        rows = []
        for i in range(101, 106):
            rows.append(
                {
                    "ls_id": str(i),
                    "month": "2025-02-01",
                    "business_gate": "gate_standard_collect" if i != 103 else "gate_soft_monitor",
                    "gate_ml_eligible": "1" if i != 103 else "0",
                    "hybrid_segment": "gate_standard_collect_c0" if i != 103 else "gate_soft_monitor__rule_only",
                }
            )

        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_uplift_prototype_outputs_and_gate_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            mart_dir = base / "mart"
            seg_dir = base / "segmentation"
            mart_dir.mkdir(parents=True, exist_ok=True)
            seg_dir.mkdir(parents=True, exist_ok=True)

            self._write_mart(mart_dir / "mart_ls_month.csv")
            self._write_hybrid_lookup(seg_dir / "segment_hybrid_assignments.csv")

            metrics = run_uplift(
                input_dir=base,
                out_dir=base,
                score_month="2025-02",
                measures_raw="sms,operator_call",
                max_train_rows=10000,
                random_state=123,
            )

            self.assertEqual(metrics["measures_trained"], 2)
            self.assertEqual(metrics["recommendations_rows"], 5)
            self.assertTrue(metrics["hybrid_lookup_used"])

            uplift_scores = base / "uplift" / "uplift_measure_scores.csv"
            uplift_recs = base / "uplift" / "uplift_recommendations.csv"
            uplift_summary = base / "uplift" / "uplift_measure_summary.csv"
            uplift_importance = base / "uplift" / "uplift_feature_importance.csv"

            self.assertTrue(uplift_scores.exists())
            self.assertTrue(uplift_recs.exists())
            self.assertTrue(uplift_summary.exists())
            self.assertTrue(uplift_importance.exists())

            with uplift_recs.open("r", encoding="utf-8", newline="") as fh:
                rows = list(csv.DictReader(fh))

            self.assertEqual(len(rows), 5)
            row_103 = next(r for r in rows if r["ls_id"] == "103")
            self.assertEqual(row_103["gate_ml_eligible"], "0")
            self.assertEqual(row_103["recommended_measure"], "")
            self.assertEqual(row_103["recommendation_reason"], "gate_ml_ineligible")


if __name__ == "__main__":
    unittest.main()
