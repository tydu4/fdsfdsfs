from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from pipeline.build_mart import run as run_build_mart
from pipeline.build_normalized import (
    _build_action_limits,
    _build_action_rows,
    _build_balance_rows,
    _build_dim_ls_profile,
    _build_payment_rows,
)
from pipeline.dq import DQCollector


class PipelineNormalizationTests(unittest.TestCase):
    def _write_raw(self, path: Path, rows: list[list[str]]) -> None:
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            col_count = max(len(r) for r in rows)
            header = ["source_file", "source_row_num"] + [f"col_{i+1}" for i in range(col_count)]
            writer.writerow(header)
            for i, row in enumerate(rows, start=1):
                payload = [path.name, str(i)] + row + [""] * (col_count - len(row))
                writer.writerow(payload)

    def test_payment_parsing_handles_comma_and_null_method(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            raw_path = Path(tmp) / "03_payment_raw.csv"
            self._write_raw(
                raw_path,
                [
                    ["Номер", "Дата оплаты", "Сумма", "Способ оплаты"],
                    ["15", "24.01.2025", "293,96", "NULL"],
                ],
            )

            dq = DQCollector(stage="normalized")
            rows = list(_build_payment_rows(raw_path, dq))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["amount_rub"], "293.96")
            self.assertEqual(rows[0]["payment_method"], "NULL")
            self.assertEqual(len(dq.issues), 0)

    def test_balance_periods_use_position_not_duplicate_header_serial(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            raw_path = Path(tmp) / "02_balance_raw.csv"
            self._write_raw(
                raw_path,
                [
                    ["ЛС", "45658", "", "", "45658", "", ""],
                    ["", "СЗ на начало", "Начислено", "Опалачено", "СЗ на начало", "Начислено", "Опалачено"],
                    ["1", "10", "5", "2", "11", "6", "3"],
                ],
            )

            dq = DQCollector(stage="normalized")
            rows = list(_build_balance_rows(raw_path, dq))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["period_month"], "2025-01-01")
            self.assertEqual(rows[1]["period_month"], "2025-02-01")

    def test_action_rows_send_invalid_ls_to_dq(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            raw_dir = Path(tmp)
            raw_path = raw_dir / "12_court_order_request_raw.csv"
            self._write_raw(
                raw_path,
                [
                    ["Заявление о выдаче судебного приказа"],
                    ["ЛС", "Дата"],
                    ["#N/A", "45868"],
                    ["66", "45868"],
                ],
            )

            dq = DQCollector(stage="normalized")
            rows = list(_build_action_rows(raw_dir, dq))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["ls_id"], "66")
            self.assertTrue(any(x.issue_type == "NON_NUMERIC_LS" for x in dq.issues))

    def test_ls_profile_dedup_uses_max_for_setup_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            raw_path = Path(tmp) / "01_ls_profile_raw.csv"
            self._write_raw(
                raw_path,
                [
                    [
                        "ЛС",
                        "Возможность дистанционного отключения",
                        "Наличие телефона",
                        "Наличие льгот",
                        "Газификация дома",
                        "Город",
                        "ЯрОблИЕРЦ квитанция",
                        "Почта России квитанция",
                        "электронная квитанция",
                        "не проживает",
                        "ЧД",
                        "МКД",
                        "Общежитие",
                        "Установка Тамбур",
                        "Установка опора",
                        "Установка в квартире/доме",
                        "Установка лестничкая клетка",
                        "Адрес (ГУИД)",
                    ],
                    ["100", "Да", "Да", "Нет", "Да", "Да", "Да", "Нет", "Да", "Нет", "Да", "Нет", "Нет", "Нет", "Нет", "Да", "Нет", ""],
                    ["100", "Да", "Да", "Нет", "Да", "Да", "Да", "Нет", "Да", "Нет", "Да", "Нет", "Нет", "Нет", "Да", "Да", "Нет", ""],
                ],
            )

            dq = DQCollector(stage="normalized")
            rows = list(_build_dim_ls_profile(raw_path, dq))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["meter_install_pole"], 1)

    def test_limits_parsing_supports_unlimited(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            raw_path = Path(tmp) / "14_action_limits_raw.csv"
            self._write_raw(
                raw_path,
                [
                    ["Наименование меры воздействия", "Лимит в месяц"],
                    ["04 Автодозвон ХК", "8000"],
                    ["05 E-mail ХК", "нет ограничений"],
                ],
            )

            dq = DQCollector(stage="normalized")
            rows = list(_build_action_limits(raw_path, dq))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["action_type"], "autodial")
            self.assertEqual(rows[0]["month_limit"], 8000)
            self.assertEqual(rows[1]["is_unlimited"], 1)


class MartLeakageTests(unittest.TestCase):
    def _write_csv(self, path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def test_mart_excludes_future_actions_from_features(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            normalized = base / "normalized"
            normalized.mkdir(parents=True, exist_ok=True)

            self._write_csv(
                normalized / "dim_ls_profile.csv",
                [
                    "ls_id",
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
                    "address_guid",
                    "source_record_count",
                ],
                [
                    {
                        "ls_id": "1",
                        "can_remote_disconnect": "1",
                        "has_phone": "1",
                        "has_benefits": "0",
                        "is_gasified": "1",
                        "is_city": "1",
                        "has_yaroblierc_receipt": "1",
                        "has_post_receipt": "0",
                        "has_e_receipt": "1",
                        "not_resident": "0",
                        "is_chd": "1",
                        "is_mkd": "0",
                        "is_dormitory": "0",
                        "meter_install_tambour": "0",
                        "meter_install_pole": "1",
                        "meter_install_apartment_house": "0",
                        "meter_install_stairwell": "0",
                        "address_guid": "",
                        "source_record_count": "1",
                    }
                ],
            )

            self._write_csv(
                normalized / "fact_balance_monthly.csv",
                ["ls_id", "period_month", "debt_start", "charge_amt", "paid_amt", "source_row_num"],
                [
                    {"ls_id": "1", "period_month": "2025-01-01", "debt_start": "100", "charge_amt": "20", "paid_amt": "5", "source_row_num": "1"},
                    {"ls_id": "1", "period_month": "2025-02-01", "debt_start": "120", "charge_amt": "10", "paid_amt": "5", "source_row_num": "2"},
                ],
            )

            self._write_csv(
                normalized / "fact_payment_txn.csv",
                ["ls_id", "payment_date", "amount_rub", "payment_method", "is_reversal", "source_row_num"],
                [
                    {"ls_id": "1", "payment_date": "2025-02-15", "amount_rub": "30", "payment_method": "5", "is_reversal": "0", "source_row_num": "1"}
                ],
            )

            self._write_csv(
                normalized / "fact_action_event.csv",
                ["ls_id", "action_date", "action_type", "event_count"],
                [
                    {"ls_id": "1", "action_date": "2025-02-10", "action_type": "sms", "event_count": "4"}
                ],
            )

            self._write_csv(
                normalized / "dim_action_limit.csv",
                ["action_type", "month_limit", "is_unlimited", "source_measure_name"],
                [{"action_type": "sms", "month_limit": "100", "is_unlimited": "0", "source_measure_name": "06 СМС"}],
            )

            out = base / "out"
            run_build_mart(base, out, "2025-01")

            mart_path = out / "mart" / "mart_ls_month.csv"
            with mart_path.open("r", encoding="utf-8", newline="") as fh:
                row = next(csv.DictReader(fh))

            self.assertEqual(row["month"], "2025-01-01")
            self.assertEqual(row["action_total_l1m"], "0")


if __name__ == "__main__":
    unittest.main()
