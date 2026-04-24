from __future__ import annotations

import io
import tempfile
import unittest
import zipfile
from pathlib import Path

from pipeline.common import find_source_file
from pipeline.constants import SOURCES
from pipeline.web_app import (
    UploadValidationError,
    UploadedFile,
    _parse_multipart_form_data,
    build_uploaded_run_package,
)


def _xlsx_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("[Content_Types].xml", "<Types></Types>")
        zf.writestr("xl/workbook.xml", "<workbook></workbook>")
    return buffer.getvalue()


class WebUploadRunTests(unittest.TestCase):
    def _write_baseline_sources(self, base_dir: Path) -> None:
        base_dir.mkdir(parents=True, exist_ok=True)
        for src in SOURCES:
            path = base_dir / f"{src.prefix}baseline.{src.kind}"
            if src.kind == "csv":
                path.write_text("header\n", encoding="utf-8")
            else:
                path.write_bytes(b"baseline-xlsx-placeholder")

    def test_uploaded_run_package_copies_base_and_overlays_uploaded_slots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base_dir = root / "base"
            runs_dir = root / "runs"
            self._write_baseline_sources(base_dir)

            manifest = build_uploaded_run_package(
                {
                    "source_02": UploadedFile("source_02", "fresh_balance.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", _xlsx_bytes()),
                    "source_03": UploadedFile("source_03", "fresh_payments.csv", "text/csv", "ЛС;Дата;Сумма\n1;01.01.2026;100\n".encode("utf-8-sig")),
                },
                base_input_dir=base_dir,
                runs_dir=runs_dir,
            )

            input_dir = Path(manifest["input_dir"])
            self.assertTrue(input_dir.exists())
            self.assertTrue(Path(manifest["output_dir"]).exists())
            self.assertEqual({item["slot"] for item in manifest["uploaded_slots"]}, {"02", "03"})

            for src in SOURCES:
                self.assertTrue(find_source_file(input_dir, src.prefix, src.kind).exists())

            self.assertEqual(find_source_file(input_dir, "02 ", "xlsx").name, "02 fresh_balance.xlsx")
            self.assertEqual(find_source_file(input_dir, "03 ", "csv").read_text(encoding="utf-8-sig").splitlines()[0], "ЛС;Дата;Сумма")

    def test_uploaded_run_ids_and_outputs_do_not_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base_dir = root / "base"
            runs_dir = root / "runs"
            self._write_baseline_sources(base_dir)
            files = {
                "source_02": UploadedFile("source_02", "balance.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", _xlsx_bytes()),
                "source_03": UploadedFile("source_03", "payments.csv", "text/csv", b"ls;date;amount\n1;2026-01-01;10\n"),
            }

            first = build_uploaded_run_package(files, base_input_dir=base_dir, runs_dir=runs_dir)
            second = build_uploaded_run_package(files, base_input_dir=base_dir, runs_dir=runs_dir)

            self.assertNotEqual(first["run_id"], second["run_id"])
            self.assertNotEqual(first["output_dir"], second["output_dir"])
            self.assertTrue(Path(first["output_dir"]).is_dir())
            self.assertTrue(Path(second["output_dir"]).is_dir())

    def test_upload_validation_rejects_wrong_extension_and_empty_file(self) -> None:
        files_wrong_ext = {
            "source_02": UploadedFile("source_02", "balance.csv", "text/csv", b"x;y\n"),
            "source_03": UploadedFile("source_03", "payments.csv", "text/csv", b"x;y\n"),
        }
        with self.assertRaises(UploadValidationError):
            build_uploaded_run_package(files_wrong_ext, base_input_dir=Path("."), runs_dir=Path("."))

        files_empty = {
            "source_02": UploadedFile("source_02", "balance.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", b""),
            "source_03": UploadedFile("source_03", "payments.csv", "text/csv", b"x;y\n"),
        }
        with self.assertRaises(UploadValidationError):
            build_uploaded_run_package(files_empty, base_input_dir=Path("."), runs_dir=Path("."))

    def test_upload_validation_requires_balance_and_payments(self) -> None:
        with self.assertRaises(UploadValidationError):
            build_uploaded_run_package(
                {"source_02": UploadedFile("source_02", "balance.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", _xlsx_bytes())},
                base_input_dir=Path("."),
                runs_dir=Path("."),
            )

    def test_multipart_parser_extracts_uploaded_files(self) -> None:
        boundary = "----codexboundary"
        body = (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="source_02"; filename="balance.xlsx"\r\n'
            "Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\r\n\r\n"
        ).encode("utf-8") + _xlsx_bytes() + (
            f"\r\n--{boundary}\r\n"
            'Content-Disposition: form-data; name="source_03"; filename="payments.csv"\r\n'
            "Content-Type: text/csv\r\n\r\n"
            "a;b\n1;2\n"
            "\r\n"
            f"--{boundary}--\r\n"
        ).encode("utf-8")

        files = _parse_multipart_form_data(f"multipart/form-data; boundary={boundary}", body)

        self.assertEqual(files["source_02"].filename, "balance.xlsx")
        self.assertEqual(files["source_03"].filename, "payments.csv")
        self.assertIn(b"a;b", files["source_03"].data)


if __name__ == "__main__":
    unittest.main()
