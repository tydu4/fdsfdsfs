"""File upload handling: multipart parsing, validation, run package creation."""
from __future__ import annotations

import csv, io, json, re, shutil, uuid, zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from email.parser import BytesParser
from email.policy import default as email_policy
from pathlib import Path
from typing import Any

from ..common import ensure_dir, find_source_file
from ..constants import SOURCES, SourceSpec
from ..settings import RUNS_DIR, SOURCE_DIR


@dataclass(frozen=True)
class UploadSlot:
    key: str
    title: str
    spec: SourceSpec
    required_for_upload: bool = False
    recommended_for_upload: bool = False


@dataclass(frozen=True)
class UploadedFile:
    field_name: str
    filename: str
    content_type: str
    data: bytes


class UploadValidationError(ValueError):
    pass


SOURCE_TITLES = {
    "01": "Профиль лицевых счетов", "02": "Баланс / оборотно-сальдовая ведомость",
    "03": "Оплаты", "04": "Автодозвон", "05": "E-mail", "06": "СМС",
    "07": "Обзвон оператором", "08": "Претензия", "09": "Выезд к абоненту",
    "10": "Уведомление об ограничении", "11": "Ограничение",
    "12": "Заявление о судебном приказе", "13": "Получение судебного приказа / ИЛ",
    "14": "Лимиты мер воздействия",
}
REQUIRED_UPLOAD_KEYS = {"02", "03"}
RECOMMENDED_UPLOAD_KEYS = {f"{idx:02d}" for idx in range(4, 14)}
SOURCE_BY_KEY = {src.prefix.strip(): src for src in SOURCES}
UPLOAD_SLOTS = tuple(
    UploadSlot(key=key, title=SOURCE_TITLES.get(key, key), spec=SOURCE_BY_KEY[key],
               required_for_upload=key in REQUIRED_UPLOAD_KEYS,
               recommended_for_upload=key in RECOMMENDED_UPLOAD_KEYS)
    for key in sorted(SOURCE_BY_KEY)
)
UPLOAD_SLOT_BY_KEY = {slot.key: slot for slot in UPLOAD_SLOTS}


def _safe_upload_name(filename: str) -> str:
    name = Path(filename.replace("\\", "/")).name
    name = re.sub(r"[^0-9A-Za-zА-Яа-яЁё._ -]+", "_", name).strip(" .")
    return name or "upload"


def _slot_key_from_field(field_name: str) -> str | None:
    value = field_name.strip().lower()
    for pattern in (r"^(?:source|slot)_(\d{2})$", r"^(\d{2})$"):
        match = re.match(pattern, value)
        if match:
            key = match.group(1)
            return key if key in UPLOAD_SLOT_BY_KEY else None
    return None


def _expected_suffix(slot: UploadSlot) -> str:
    return f".{slot.spec.kind}"


def parse_multipart_form_data(content_type: str, body: bytes) -> dict[str, UploadedFile]:
    if "multipart/form-data" not in content_type.lower():
        raise UploadValidationError("Ожидался multipart/form-data запрос с файлами.")
    message = BytesParser(policy=email_policy).parsebytes(
        (f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n").encode("utf-8") + body
    )
    if not message.is_multipart():
        raise UploadValidationError("Не удалось разобрать multipart-форму.")
    files: dict[str, UploadedFile] = {}
    for part in message.iter_parts():
        disposition = part.get("Content-Disposition", "")
        if "form-data" not in disposition:
            continue
        params = dict(part.get_params(header="content-disposition", unquote=True) or [])
        field_name = str(params.get("name") or "").strip()
        filename = str(params.get("filename") or "").strip()
        if not field_name or not filename:
            continue
        payload = part.get_payload(decode=True) or b""
        files[field_name] = UploadedFile(field_name=field_name, filename=filename,
                                          content_type=part.get_content_type(), data=payload)
    return files


def _validate_uploaded_file(slot: UploadSlot, upload: UploadedFile) -> None:
    expected_suffix = _expected_suffix(slot)
    filename = _safe_upload_name(upload.filename)
    if Path(filename).suffix.lower() != expected_suffix:
        raise UploadValidationError(f"{slot.key} {slot.title}: ожидается файл {expected_suffix}, получен '{upload.filename}'.")
    if len(upload.data) == 0:
        raise UploadValidationError(f"{slot.key} {slot.title}: загруженный файл пустой.")
    if expected_suffix == ".xlsx":
        if not zipfile.is_zipfile(io.BytesIO(upload.data)):
            raise UploadValidationError(f"{slot.key} {slot.title}: файл не похож на корректный XLSX.")
        return
    if expected_suffix == ".csv":
        try:
            text = upload.data.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise UploadValidationError(f"{slot.key} {slot.title}: CSV должен быть в UTF-8 или UTF-8-SIG.") from exc
        reader = csv.reader(io.StringIO(text), delimiter=";")
        has_row = any(any(cell.strip() for cell in row) for row in reader)
        if not has_row:
            raise UploadValidationError(f"{slot.key} {slot.title}: CSV не содержит данных.")


def _normalize_uploaded_files(files: Mapping[str, UploadedFile]) -> dict[str, UploadedFile]:
    uploads_by_slot: dict[str, UploadedFile] = {}
    for field_name, upload in files.items():
        slot_key = _slot_key_from_field(field_name)
        if slot_key is None:
            continue
        if slot_key in uploads_by_slot:
            raise UploadValidationError(f"Файл для слота {slot_key} загружен несколько раз.")
        uploads_by_slot[slot_key] = upload
    missing = sorted(REQUIRED_UPLOAD_KEYS - set(uploads_by_slot))
    if missing:
        labels = ", ".join(f"{key} {UPLOAD_SLOT_BY_KEY[key].title}" for key in missing)
        raise UploadValidationError(f"Для нового скоринга нужно загрузить обязательные файлы: {labels}.")
    for slot_key, upload in uploads_by_slot.items():
        _validate_uploaded_file(UPLOAD_SLOT_BY_KEY[slot_key], upload)
    return uploads_by_slot


def _copy_baseline_sources(base_input_dir: Path, run_input_dir: Path) -> None:
    ensure_dir(run_input_dir)
    for src in SOURCES:
        source_file = find_source_file(base_input_dir, src.prefix, src.kind)
        shutil.copy2(source_file, run_input_dir / source_file.name)


def _store_uploaded_file(run_input_dir: Path, slot: UploadSlot, upload: UploadedFile) -> dict[str, Any]:
    expected_suffix = _expected_suffix(slot)
    for existing in run_input_dir.iterdir():
        if existing.is_file() and existing.name.startswith(slot.spec.prefix) and existing.suffix.lower() == expected_suffix:
            existing.unlink()
    safe_name = _safe_upload_name(upload.filename)
    if Path(safe_name).suffix.lower() != expected_suffix:
        safe_name = f"{Path(safe_name).stem}{expected_suffix}"
    if not safe_name.startswith(slot.spec.prefix):
        safe_name = f"{slot.spec.prefix}{safe_name}"
    target = run_input_dir / safe_name
    target.write_bytes(upload.data)
    return {"slot": slot.key, "title": slot.title, "filename": upload.filename,
            "stored_as": target.name, "size_bytes": len(upload.data), "required": slot.required_for_upload}


def build_uploaded_run_package(
    files: Mapping[str, UploadedFile],
    base_input_dir: Path | None = None,
    runs_dir: Path | None = None,
) -> dict[str, Any]:
    base_input_dir = Path(base_input_dir or SOURCE_DIR)
    runs_dir = Path(runs_dir or RUNS_DIR)
    uploads_by_slot = _normalize_uploaded_files(files)
    run_id = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    run_root = runs_dir / run_id
    run_input_dir = run_root / "input"
    run_output_dir = run_root / "output"
    ensure_dir(run_output_dir)
    _copy_baseline_sources(base_input_dir, run_input_dir)
    uploaded_slots = []
    for slot_key in sorted(uploads_by_slot):
        uploaded_slots.append(_store_uploaded_file(run_input_dir, UPLOAD_SLOT_BY_KEY[slot_key], uploads_by_slot[slot_key]))
    manifest = {
        "run_id": run_id, "mode": "overlay_full_run",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "base_input_dir": str(base_input_dir), "input_dir": str(run_input_dir),
        "output_dir": str(run_output_dir), "required_slots": sorted(REQUIRED_UPLOAD_KEYS),
        "uploaded_slots": uploaded_slots,
    }
    (run_root / "run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
