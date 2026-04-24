from __future__ import annotations

import csv
import io
import json
import mimetypes
import re
import shutil
import socket
import threading
import time
import traceback
import uuid
import webbrowser
import zipfile
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from email.parser import BytesParser
from email.policy import default as email_policy
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from . import run_pipeline
from .common import ensure_dir, find_source_file
from .constants import SOURCES, SourceSpec
from .settings import HOST, OUTPUT_DIR, PORT, RUNS_DIR, SOURCE_DIR


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
    "01": "Профиль лицевых счетов",
    "02": "Баланс / оборотно-сальдовая ведомость",
    "03": "Оплаты",
    "04": "Автодозвон",
    "05": "E-mail",
    "06": "СМС",
    "07": "Обзвон оператором",
    "08": "Претензия",
    "09": "Выезд к абоненту",
    "10": "Уведомление об ограничении",
    "11": "Ограничение",
    "12": "Заявление о судебном приказе",
    "13": "Получение судебного приказа / ИЛ",
    "14": "Лимиты мер воздействия",
}

REQUIRED_UPLOAD_KEYS = {"02", "03"}
RECOMMENDED_UPLOAD_KEYS = {f"{idx:02d}" for idx in range(4, 14)}
SOURCE_BY_KEY = {src.prefix.strip(): src for src in SOURCES}
UPLOAD_SLOTS = tuple(
    UploadSlot(
        key=key,
        title=SOURCE_TITLES.get(key, key),
        spec=SOURCE_BY_KEY[key],
        required_for_upload=key in REQUIRED_UPLOAD_KEYS,
        recommended_for_upload=key in RECOMMENDED_UPLOAD_KEYS,
    )
    for key in sorted(SOURCE_BY_KEY)
)
UPLOAD_SLOT_BY_KEY = {slot.key: slot for slot in UPLOAD_SLOTS}
RUN_ID_RE = re.compile(r"^[0-9]{8}-[0-9]{6}-[0-9a-f]{8}$")


class PipelineState:
    def __init__(
        self,
        run_id: str = "default",
        input_dir: Path = SOURCE_DIR,
        output_dir: Path = OUTPUT_DIR,
        manifest: Mapping[str, Any] | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self.run_id = run_id
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.manifest = dict(manifest or {})
        self._running = False
        self._started_at: str | None = None
        self._finished_at: str | None = None
        self._last_error: str | None = None
        self._month: str | None = None
        self._stages = self._initial_stages()

    def _initial_stages(self) -> list[dict[str, Any]]:
        return [
            {
                "name": stage["name"],
                "title": stage["title"],
                "description": stage["description"],
                "status": "not_started",
                "duration_sec": None,
                "metrics": {},
                "error": None,
            }
            for stage in run_pipeline.STAGES
        ]

    def start(self) -> bool:
        with self._lock:
            if self._running:
                return False
            self._running = True
            self._started_at = datetime.now().isoformat(timespec="seconds")
            self._finished_at = None
            self._last_error = None
            self._month = None
            self._stages = self._initial_stages()
            return True

    def finish(self, error: str | None = None) -> None:
        with self._lock:
            self._running = False
            self._finished_at = datetime.now().isoformat(timespec="seconds")
            if error:
                self._last_error = error

    def handle_progress(self, event: dict[str, Any]) -> None:
        with self._lock:
            if event.get("status") == "month_detected":
                self._month = event.get("month")
                return
            if event.get("status") == "pipeline_completed":
                self._month = event.get("month")
                return

            stage_name = event.get("stage")
            if not stage_name:
                return

            for stage in self._stages:
                if stage["name"] == stage_name:
                    stage["status"] = event.get("status", stage["status"])
                    stage["duration_sec"] = event.get("duration_sec", stage["duration_sec"])
                    stage["metrics"] = event.get("metrics", stage["metrics"])
                    stage["error"] = event.get("error")
                    if stage["error"]:
                        self._last_error = stage["error"]
                    break

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            stages = json.loads(json.dumps(self._stages, ensure_ascii=False))
            running = self._running
            payload = {
                "run_id": self.run_id,
                "mode": self.manifest.get("mode", "default_full_run"),
                "created_at": self.manifest.get("created_at"),
                "input_dir": str(self.input_dir),
                "output_dir": str(self.output_dir),
                "uploaded_slots": self.manifest.get("uploaded_slots", []),
                "running": running,
                "started_at": self._started_at,
                "finished_at": self._finished_at,
                "last_error": self._last_error,
                "month": self._month,
                "stages": stages,
                "outputs": _collect_outputs(self.output_dir),
                "decision": _collect_decision_summary(self.output_dir),
                "last_metrics": _read_json(self.output_dir / "load_metrics_pipeline.json"),
            }

        if not running:
            _hydrate_from_last_metrics(payload, self.output_dir)
        return payload


STATE = PipelineState()
RUN_STATES: dict[str, PipelineState] = {"default": STATE}
RUN_STATES_LOCK = threading.RLock()
ACTIVE_RUN_ID: str | None = None
_DECISION_CACHE: dict[str, Any] = {"signature": None, "payload": None}


def _safe_upload_name(filename: str) -> str:
    name = Path(filename.replace("\\", "/")).name
    name = re.sub(r"[^0-9A-Za-zА-Яа-яЁё._ -]+", "_", name).strip(" .")
    return name or "upload"


def _slot_key_from_field(field_name: str) -> str | None:
    value = field_name.strip().lower()
    match = re.match(r"^(?:source|slot)_(\d{2})$", value)
    if match:
        key = match.group(1)
        return key if key in UPLOAD_SLOT_BY_KEY else None
    match = re.match(r"^(\d{2})$", value)
    if match:
        key = match.group(1)
        return key if key in UPLOAD_SLOT_BY_KEY else None
    return None


def _expected_suffix(slot: UploadSlot) -> str:
    return f".{slot.spec.kind}"


def _parse_multipart_form_data(content_type: str, body: bytes) -> dict[str, UploadedFile]:
    if "multipart/form-data" not in content_type.lower():
        raise UploadValidationError("Ожидался multipart/form-data запрос с файлами.")

    message = BytesParser(policy=email_policy).parsebytes(
        (
            f"Content-Type: {content_type}\r\n"
            "MIME-Version: 1.0\r\n"
            "\r\n"
        ).encode("utf-8")
        + body
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
        files[field_name] = UploadedFile(
            field_name=field_name,
            filename=filename,
            content_type=part.get_content_type(),
            data=payload,
        )
    return files


def _validate_uploaded_file(slot: UploadSlot, upload: UploadedFile) -> None:
    expected_suffix = _expected_suffix(slot)
    filename = _safe_upload_name(upload.filename)
    if Path(filename).suffix.lower() != expected_suffix:
        raise UploadValidationError(
            f"{slot.key} {slot.title}: ожидается файл {expected_suffix}, получен '{upload.filename}'."
        )
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
    return {
        "slot": slot.key,
        "title": slot.title,
        "filename": upload.filename,
        "stored_as": target.name,
        "size_bytes": len(upload.data),
        "required": slot.required_for_upload,
    }


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
        "run_id": run_id,
        "mode": "overlay_full_run",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "base_input_dir": str(base_input_dir),
        "input_dir": str(run_input_dir),
        "output_dir": str(run_output_dir),
        "required_slots": sorted(REQUIRED_UPLOAD_KEYS),
        "uploaded_slots": uploaded_slots,
    }
    (run_root / "run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def _validate_run_id(run_id: str) -> str:
    if run_id == "default":
        return run_id
    if not RUN_ID_RE.match(run_id):
        raise KeyError(run_id)
    return run_id


def _load_run_manifest(run_id: str) -> dict[str, Any] | None:
    if run_id == "default":
        return None
    _validate_run_id(run_id)
    manifest_path = RUNS_DIR / run_id / "run_manifest.json"
    if not manifest_path.exists():
        return None
    return _read_json(manifest_path)


def get_run_state(run_id: str) -> PipelineState | None:
    run_id = _validate_run_id(run_id)
    with RUN_STATES_LOCK:
        if run_id in RUN_STATES:
            return RUN_STATES[run_id]
        manifest = _load_run_manifest(run_id)
        if manifest is None:
            return None
        state = PipelineState(
            run_id=run_id,
            input_dir=Path(manifest["input_dir"]),
            output_dir=Path(manifest["output_dir"]),
            manifest=manifest,
        )
        RUN_STATES[run_id] = state
        return state


def _list_runs() -> list[dict[str, Any]]:
    runs = [
        {
            "run_id": "default",
            "created_at": None,
            "mode": "default_full_run",
            "uploaded_slots": [],
            "status_url": "/api/status",
        }
    ]
    if RUNS_DIR.exists():
        for manifest_path in sorted(RUNS_DIR.glob("*/run_manifest.json"), reverse=True):
            manifest = _read_json(manifest_path)
            if not manifest:
                continue
            run_id = str(manifest.get("run_id", manifest_path.parent.name))
            state = get_run_state(run_id)
            snapshot = state.snapshot() if state else {}
            runs.append(
                {
                    "run_id": run_id,
                    "created_at": manifest.get("created_at"),
                    "mode": manifest.get("mode", "overlay_full_run"),
                    "uploaded_slots": manifest.get("uploaded_slots", []),
                    "running": snapshot.get("running", False),
                    "last_error": snapshot.get("last_error"),
                    "month": snapshot.get("month"),
                    "status_url": f"/api/runs/{run_id}/status",
                    "results_url": f"/api/runs/{run_id}/results",
                }
            )
    return runs


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _file_size_mb(path: Path) -> float:
    return round(path.stat().st_size / (1024 * 1024), 2)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _to_float(value: Any, default: float = 0.0) -> float:
    text = str(value if value is not None else "").strip()
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    return int(round(_to_float(value, float(default))))


def _pct(part: int | float, total: int | float) -> float:
    total_value = float(total)
    if total_value <= 0:
        return 0.0
    return round(100.0 * float(part) / total_value, 2)


def _file_signature(paths: list[Path]) -> tuple[tuple[str, int, int], ...]:
    signature = []
    for path in paths:
        if path.exists():
            stat = path.stat()
            signature.append((str(path), stat.st_mtime_ns, stat.st_size))
        else:
            signature.append((str(path), 0, 0))
    return tuple(signature)


def _pretty_gate(raw: str) -> str:
    labels = {
        "gate_closed_no_debt": "Нет активного долга",
        "gate_soft_monitor": "Мягкий мониторинг",
        "gate_standard_collect": "Стандартное взыскание",
        "gate_restriction_eligible": "Доступно ограничение",
        "gate_prelegal_priority": "Предюридический приоритет",
        "gate_contact_deficit": "Дефицит контактов",
    }
    return labels.get(raw, raw.replace("gate_", "").replace("_", " ").title())


def _pretty_measure(raw: str) -> str:
    labels = {
        "sms": "СМС",
        "operator_call": "Звонок оператора",
        "restriction_notice": "Уведомление об ограничении",
        "no_action": "Без активной меры",
        "": "Без активной меры",
    }
    return labels.get(raw, raw.replace("_", " ").title())


def _pretty_reason(raw: str) -> str:
    labels = {
        "gate_ml_ineligible": "закрыто бизнес-правилом",
        "non_positive_uplift": "модель не видит положительного эффекта",
        "max_uplift": "лучший положительный uplift",
    }
    return labels.get(raw, raw.replace("_", " "))


def _gate_rationale(gate: str) -> str:
    rationales = {
        "gate_closed_no_debt": "Активное взыскание нерационально: долга нет или он неположительный, поэтому мера несет только операционные и юридические риски.",
        "gate_soft_monitor": "Небольшой долг и недавние оплаты лучше вести мягко: агрессивная мера может стоить дороже ожидаемого возврата.",
        "gate_standard_collect": "Базовый сценарий: долг есть, жестких ограничений нет, поэтому решение выбирается по ожидаемому эффекту модели.",
        "gate_restriction_eligible": "Есть техническая возможность ограничения, поэтому уведомление допустимо, но выбирается только при положительном uplift.",
        "gate_prelegal_priority": "Высокий долг без свежих оплат требует приоритизации: здесь важны скорость реакции и юридическая корректность.",
        "gate_contact_deficit": "Контактных каналов мало, поэтому выбор меры должен учитывать риск недоставки и лишней операционной нагрузки.",
    }
    return rationales.get(gate, "Профиль обрабатывается по общим правилам портфеля и прогнозному эффекту модели.")


def _measure_constraints(measure: str) -> str:
    constraints = {
        "sms": "Низкая стоимость и низкий юридический риск, но эффект ограничен качеством контактных данных.",
        "operator_call": "Средняя стоимость и ограниченная емкость операторов; рационально применять там, где uplift покрывает ручную нагрузку.",
        "restriction_notice": "Более высокий юридический и репутационный риск; рационально только для допустимых профилей с ожидаемым эффектом.",
        "no_action": "Экономит бюджет мер и снижает риск лишнего воздействия, когда модель или бизнес-правила не поддерживают активное действие.",
        "": "Экономит бюджет мер и снижает риск лишнего воздействия, когда модель или бизнес-правила не поддерживают активное действие.",
    }
    return constraints.get(measure, "Мера оценивается по прогнозному эффекту, стоимости применения и допустимости для профиля.")


def _decision_rationale(
    gate: str,
    recommendation: str,
    reason: str,
    avg_uplift: float,
    incremental_payers: float,
    avg_debt: float,
) -> tuple[str, str]:
    measure_label = _pretty_measure(recommendation)
    if reason == "gate_ml_ineligible":
        rationale = f"{_gate_rationale(gate)} Рекомендация: {measure_label.lower()}."
    elif reason == "non_positive_uplift":
        rationale = (
            "Активная мера не назначается, потому что лучший прогнозный uplift неположительный. "
            "Рациональнее не тратить бюджет воздействия и не создавать лишний контакт с абонентом."
        )
    elif recommendation and recommendation != "no_action":
        rationale = (
            f"Выбрана мера `{measure_label}`: внутри профиля она дает лучший положительный прогнозный uplift. "
            f"{_measure_constraints(recommendation)}"
        )
    else:
        rationale = "Активная мера не выбрана: ожидаемый прирост оплаты не подтвержден текущей моделью."

    if avg_uplift > 0:
        effect = (
            f"Ожидаемый эффект: +{avg_uplift * 100:.2f} п.п. к вероятности оплаты в ближайший месяц; "
            f"примерно +{incremental_payers:.1f} вероятностных оплат по профилю. "
            f"Средний долг профиля: {avg_debt:,.0f} руб.".replace(",", " ")
        )
    else:
        effect = (
            "Ожидаемый эффект: положительный прирост оплаты в ближайший месяц не подтвержден. "
            f"Средний долг профиля: {avg_debt:,.0f} руб.; приоритет — контроль затрат и рисков.".replace(",", " ")
        )

    return rationale, effect


def _collect_decision_summary(output_dir: Path = OUTPUT_DIR) -> dict[str, Any]:
    paths = [
        output_dir / "load_metrics_pipeline.json",
        output_dir / "segmentation" / "segment_hybrid_gate_profile.csv",
        output_dir / "segmentation" / "segment_hybrid_cluster_profile.csv",
        output_dir / "segmentation" / "segment_hybrid_assignments.csv",
        output_dir / "uplift" / "uplift_measure_summary.csv",
        output_dir / "uplift" / "uplift_recommendations.csv",
        output_dir / "uplift" / "load_metrics_uplift.json",
    ]
    signature = _file_signature(paths)
    if _DECISION_CACHE["signature"] == signature:
        return _DECISION_CACHE["payload"]

    pipeline_metrics = _read_json(output_dir / "load_metrics_pipeline.json") or {}
    uplift_metrics = _read_json(output_dir / "uplift" / "load_metrics_uplift.json") or {}
    gate_rows = _read_csv_rows(output_dir / "segmentation" / "segment_hybrid_gate_profile.csv")
    cluster_rows = _read_csv_rows(output_dir / "segmentation" / "segment_hybrid_cluster_profile.csv")
    measure_rows = _read_csv_rows(output_dir / "uplift" / "uplift_measure_summary.csv")
    recommendation_rows = _read_csv_rows(output_dir / "uplift" / "uplift_recommendations.csv")
    assignment_rows = _read_csv_rows(output_dir / "segmentation" / "segment_hybrid_assignments.csv")

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
        item["debt"] += _to_float(row.get("debt_start_t"))
        item["pay_3m"] += _to_float(row.get("payment_amount_l3m"))
        item["actions_3m"] += _to_float(row.get("action_total_l3m"))
        item["contacts"] += _to_float(row.get("contact_channels_count"))

    profiles: dict[str, dict[str, Any]] = {}
    for row in recommendation_rows:
        profile = row.get("hybrid_segment", "") or row.get("business_gate", "") or "unknown_profile"
        gate = row.get("business_gate", "") or "unknown_gate"
        recommendation = (row.get("recommended_measure", "") or "").strip() or "no_action"
        reason = (row.get("recommendation_reason", "") or "").strip()
        uplift = _to_float(row.get("recommended_uplift"), 0.0) if recommendation != "no_action" else 0.0

        rec_counts[recommendation] += 1
        reason_counts[reason] += 1
        if recommendation != "no_action" and uplift > 0:
            positive_actions += 1
            positive_uplift_sum += uplift

        item = profiles.setdefault(
            profile,
            {
                "profile": profile,
                "business_gate": gate,
                "accounts": 0,
                "recommendations": Counter(),
                "reasons": Counter(),
                "positive_uplift_sum": 0.0,
                "positive_uplift_count": 0,
            },
        )
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
        rationale, economic_effect = _decision_rationale(
            gate=item["business_gate"],
            recommendation=recommendation,
            reason=reason,
            avg_uplift=avg_uplift,
            incremental_payers=item["positive_uplift_sum"],
            avg_debt=avg_debt,
        )

        profile_cards.append(
            {
                "profile": profile,
                "profile_label": profile.replace("gate_", "").replace("__rule_only", " rule-only").replace("_", " "),
                "business_gate": item["business_gate"],
                "business_gate_label": _pretty_gate(item["business_gate"]),
                "accounts": int(item["accounts"]),
                "share_pct": _pct(item["accounts"], total_accounts),
                "top_recommendation": recommendation,
                "top_recommendation_label": _pretty_measure(recommendation),
                "top_recommendation_count": int(recommendation_count),
                "top_recommendation_share_pct": _pct(recommendation_count, item["accounts"]),
                "dominant_reason": reason,
                "dominant_reason_label": _pretty_reason(reason),
                "dominant_reason_count": int(reason_count),
                "avg_positive_uplift_pct": round(avg_uplift * 100.0, 2),
                "expected_incremental_payers": round(float(item["positive_uplift_sum"]), 1),
                "avg_debt": round(avg_debt, 2),
                "avg_payment_3m": round(avg_pay_3m, 2),
                "avg_actions_3m": round(avg_actions_3m, 2),
                "avg_contacts": round(avg_contacts, 2),
                "rationale": rationale,
                "economic_effect": economic_effect,
                "business_constraints": _measure_constraints(recommendation),
            }
        )

    profile_cards.sort(key=lambda x: x["accounts"], reverse=True)

    gates = []
    for row in gate_rows:
        gate = row.get("business_gate", "")
        gates.append(
            {
                "business_gate": gate,
                "label": _pretty_gate(gate),
                "accounts": _to_int(row.get("ls_count")),
                "share_pct": _to_float(row.get("share_total_pct")),
                "ml_eligible": _to_int(row.get("ml_eligible_ls_count")),
                "ml_eligible_share_pct": _to_float(row.get("ml_eligible_share_pct")),
                "reason": row.get("top_gate_reason", ""),
                "rationale": _gate_rationale(gate),
            }
        )
    gates.sort(key=lambda x: x["accounts"], reverse=True)

    model_quality = []
    for row in measure_rows:
        measure = row.get("measure", "")
        ate_dr = _to_float(row.get("ate_dr"))
        verdict = "Положительный средний эффект" if ate_dr > 0 else "Средний эффект неположительный"
        model_quality.append(
            {
                "measure": measure,
                "measure_label": _pretty_measure(measure),
                "train_rows": _to_int(row.get("train_rows")),
                "treated_rows": _to_int(row.get("train_treated_rows")),
                "treated_share_pct": round(_to_float(row.get("train_treated_share")) * 100.0, 2),
                "outcome_rate_pct": round(_to_float(row.get("train_outcome_rate")) * 100.0, 2),
                "treated_outcome_pct": round(_to_float(row.get("treated_outcome_rate")) * 100.0, 2),
                "control_outcome_pct": round(_to_float(row.get("control_outcome_rate")) * 100.0, 2),
                "naive_ate_pct": round(_to_float(row.get("naive_ate")) * 100.0, 2),
                "ate_dr_pct": round(ate_dr * 100.0, 2),
                "avg_pred_uplift_pct": round(_to_float(row.get("avg_pred_uplift_train")) * 100.0, 2),
                "score_rows": _to_int(row.get("score_rows")),
                "verdict": verdict,
                "constraints": _measure_constraints(measure),
            }
        )
    model_quality.sort(key=lambda x: x["ate_dr_pct"], reverse=True)

    rec_mix = [
        {
            "measure": measure,
            "label": _pretty_measure(measure),
            "accounts": count,
            "share_pct": _pct(count, total_accounts),
            "constraints": _measure_constraints(measure),
        }
        for measure, count in rec_counts.most_common()
    ]

    cluster_mix_by_gate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in cluster_rows:
        gate = row.get("business_gate", "")
        cluster_mix_by_gate[gate].append(
            {
                "cluster_id": _to_int(row.get("hybrid_cluster_id")),
                "accounts": _to_int(row.get("ls_count")),
                "share_within_gate_pct": _to_float(row.get("share_within_gate_pct")),
            }
        )

    payload = {
        "score_month": str(pipeline_metrics.get("month") or uplift_metrics.get("score_month") or "")[:7],
        "overview": {
            "total_accounts": total_accounts,
            "positive_actions": positive_actions,
            "positive_actions_share_pct": _pct(positive_actions, total_accounts),
            "no_action": rec_counts.get("no_action", 0),
            "no_action_share_pct": _pct(rec_counts.get("no_action", 0), total_accounts),
            "rule_blocked": reason_counts.get("gate_ml_ineligible", 0),
            "rule_blocked_share_pct": _pct(reason_counts.get("gate_ml_ineligible", 0), total_accounts),
            "non_positive_uplift": reason_counts.get("non_positive_uplift", 0),
            "non_positive_uplift_share_pct": _pct(reason_counts.get("non_positive_uplift", 0), total_accounts),
            "expected_incremental_payers": round(positive_uplift_sum, 1),
        },
        "gates": gates,
        "profiles": profile_cards,
        "profile_count": len(profile_cards),
        "recommendation_mix": rec_mix,
        "reason_mix": [
            {"reason": reason, "label": _pretty_reason(reason), "accounts": count, "share_pct": _pct(count, total_accounts)}
            for reason, count in reason_counts.most_common()
        ],
        "model_quality": model_quality,
        "cluster_mix_by_gate": cluster_mix_by_gate,
    }
    _DECISION_CACHE["signature"] = signature
    _DECISION_CACHE["payload"] = payload
    return payload


def _rows_from_metrics(output_dir: Path = OUTPUT_DIR) -> dict[str, int]:
    rows: dict[str, int] = {}

    raw_metrics = _read_json(output_dir / "raw" / "load_metrics_raw.json") or {}
    for item in raw_metrics.get("sources", []):
        output_name = item.get("raw_output")
        if output_name:
            rows[str(output_name)] = int(item.get("rows_written", 0))

    for metrics_path in output_dir.glob("*/load_metrics_*.json"):
        metrics = _read_json(metrics_path) or {}
        for table in metrics.get("tables", []):
            csv_name = table.get("csv")
            if csv_name:
                rows[str(csv_name)] = int(table.get("rows", 0))

    return rows


def _collect_csv_outputs(output_dir: Path = OUTPUT_DIR) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not output_dir.exists():
        return rows
    row_counts = _rows_from_metrics(output_dir)
    for path in sorted(output_dir.glob("*/*.csv")):
        rel = path.relative_to(OUTPUT_DIR).as_posix()
        rows.append(
            {
                "name": path.name,
                "layer": path.parent.name,
                "rows": row_counts.get(path.name),
                "size_mb": _file_size_mb(path),
                "url": f"/outputs/{rel}",
            }
        )
    return rows


def _collect_outputs(output_dir: Path = OUTPUT_DIR) -> dict[str, Any]:
    return {
        "tables": _collect_csv_outputs(output_dir),
    }


def _hydrate_from_last_metrics(payload: dict[str, Any], output_dir: Path = OUTPUT_DIR) -> None:
    metrics = payload.get("last_metrics")
    if not metrics:
        _hydrate_from_stage_metrics(payload, output_dir)
        return
    payload["month"] = payload.get("month") or metrics.get("month")
    completed_by_name = {item.get("stage"): item for item in metrics.get("stages", [])}
    for stage in payload["stages"]:
        saved = completed_by_name.get(stage["name"])
        if saved and stage["status"] == "not_started":
            stage["status"] = "completed"
            stage["duration_sec"] = saved.get("duration_sec")
            stage["metrics"] = saved.get("metrics", {})


def _hydrate_from_stage_metrics(payload: dict[str, Any], output_dir: Path = OUTPUT_DIR) -> None:
    stage_metric_paths = {
        "prepare_raw": output_dir / "raw" / "load_metrics_raw.json",
        "build_normalized": output_dir / "normalized" / "load_metrics_normalized.json",
        "build_mart": output_dir / "mart" / "load_metrics_mart.json",
        "build_segmentation": output_dir / "segmentation" / "load_metrics_segmentation.json",
        "build_uplift_prototype": output_dir / "uplift" / "load_metrics_uplift.json",
        "build_presentation_viz": output_dir / "presentation" / "load_metrics_presentation_viz.json",
        "build_presentation_notes": output_dir / "presentation" / "load_metrics_presentation_notes.json",
    }
    for stage in payload["stages"]:
        if stage["status"] != "not_started":
            continue
        metrics = _read_json(stage_metric_paths[stage["name"]])
        if not metrics:
            continue
        stage["status"] = "completed"
        stage["metrics"] = metrics
        payload["month"] = payload.get("month") or _month_from_metrics(metrics)


def _month_from_metrics(metrics: dict[str, Any]) -> str | None:
    for key in ("as_of_month", "target_month", "score_month"):
        value = metrics.get(key)
        if value:
            return str(value)[:7]
    return None


def _run_pipeline_worker(state: PipelineState, overrides: Mapping[str, Any] | None = None) -> None:
    global ACTIVE_RUN_ID
    try:
        run_pipeline.run(overrides=overrides, progress_callback=state.handle_progress)
    except Exception as exc:
        traceback.print_exc()
        state.finish(error=str(exc))
        with RUN_STATES_LOCK:
            if ACTIVE_RUN_ID == state.run_id:
                ACTIVE_RUN_ID = None
        return
    state.finish()
    with RUN_STATES_LOCK:
        if ACTIVE_RUN_ID == state.run_id:
            ACTIVE_RUN_ID = None


def start_pipeline_run(state: PipelineState = STATE, overrides: Mapping[str, Any] | None = None) -> bool:
    global ACTIVE_RUN_ID
    with RUN_STATES_LOCK:
        if ACTIVE_RUN_ID is not None:
            return False
        if not state.start():
            return False
        RUN_STATES[state.run_id] = state
        ACTIVE_RUN_ID = state.run_id

    thread = threading.Thread(target=_run_pipeline_worker, args=(state, overrides), daemon=True)
    thread.start()
    return True


def _find_free_port(host: str, preferred_port: int) -> int:
    for port in range(preferred_port, preferred_port + 50):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind((host, port))
            except OSError:
                continue
            return port
    raise RuntimeError(f"No free localhost port found starting from {preferred_port}")


def _write_server_marker(url: str, port: int) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    marker = {
        "url": url,
        "host": HOST,
        "port": port,
        "started_at": datetime.now().isoformat(timespec="seconds"),
    }
    (OUTPUT_DIR / "web_interface.json").write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")


def _html() -> bytes:
    return HTML.encode("utf-8")


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "DebtPipelineDashboard/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_json(self, payload: Any, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_bytes(self, body: bytes, content_type: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/":
            self._send_bytes(_html(), "text/html; charset=utf-8")
            return
        if path == "/api/status":
            self._send_json(STATE.snapshot())
            return
        if path == "/api/runs":
            self._send_json({"runs": _list_runs()})
            return
        run_match = re.match(r"^/api/runs/([^/]+)/(status|results)$", path)
        if run_match:
            run_id = unquote(run_match.group(1))
            try:
                state = get_run_state(run_id)
            except KeyError:
                state = None
            if state is None:
                self._send_json({"error": "run_not_found"}, HTTPStatus.NOT_FOUND)
                return
            if run_match.group(2) == "status":
                self._send_json(state.snapshot())
                return
            self._send_json(
                {
                    "run_id": state.run_id,
                    "decision": _collect_decision_summary(state.output_dir),
                    "outputs": _collect_outputs(state.output_dir),
                    "last_metrics": _read_json(state.output_dir / "load_metrics_pipeline.json"),
                }
            )
            return
        if path.startswith("/outputs/"):
            self._serve_output(path)
            return
        self._send_json({"error": "not_found"}, HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path == "/api/run":
            if not start_pipeline_run():
                self._send_json({"error": "pipeline_already_running"}, HTTPStatus.CONFLICT)
                return
            self._send_json({"ok": True, "run_id": "default"})
            return
        if path == "/api/runs":
            self._handle_create_run()
            return
        self._send_json({"error": "not_found"}, HTTPStatus.NOT_FOUND)

    def _handle_create_run(self) -> None:
        with RUN_STATES_LOCK:
            if ACTIVE_RUN_ID is not None:
                self._send_json({"error": "pipeline_already_running", "active_run_id": ACTIVE_RUN_ID}, HTTPStatus.CONFLICT)
                return

        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            content_length = 0
        if content_length <= 0:
            self._send_json({"error": "empty_request"}, HTTPStatus.BAD_REQUEST)
            return

        try:
            body = self.rfile.read(content_length)
            files = _parse_multipart_form_data(self.headers.get("Content-Type", ""), body)
            manifest = build_uploaded_run_package(files)
        except UploadValidationError as exc:
            self._send_json({"error": "validation_error", "message": str(exc)}, HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:
            traceback.print_exc()
            self._send_json({"error": "upload_failed", "message": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        state = PipelineState(
            run_id=str(manifest["run_id"]),
            input_dir=Path(manifest["input_dir"]),
            output_dir=Path(manifest["output_dir"]),
            manifest=manifest,
        )
        overrides = {
            "input_dir": manifest["input_dir"],
            "out_dir": manifest["output_dir"],
            "presentation": {
                "out_dir": str(Path(manifest["output_dir"]) / "presentation"),
            },
        }
        if not start_pipeline_run(state=state, overrides=overrides):
            self._send_json({"error": "pipeline_already_running"}, HTTPStatus.CONFLICT)
            return

        self._send_json(
            {
                "ok": True,
                "run_id": state.run_id,
                "status_url": f"/api/runs/{state.run_id}/status",
                "results_url": f"/api/runs/{state.run_id}/results",
                "uploaded_slots": manifest["uploaded_slots"],
            },
            HTTPStatus.CREATED,
        )

    def _serve_output(self, path: str) -> None:
        rel = unquote(path.removeprefix("/outputs/"))
        target = (OUTPUT_DIR / rel).resolve()
        output_root = OUTPUT_DIR.resolve()
        if target != output_root and output_root not in target.parents:
            self._send_json({"error": "forbidden"}, HTTPStatus.FORBIDDEN)
            return
        if not target.exists() or not target.is_file():
            self._send_json({"error": "not_found"}, HTTPStatus.NOT_FOUND)
            return
        content_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        self._send_bytes(target.read_bytes(), content_type)


def run_server(open_browser: bool = True) -> str:
    port = _find_free_port(HOST, PORT)
    url = f"http://{HOST}:{port}"
    _write_server_marker(url, port)
    server = ThreadingHTTPServer((HOST, port), DashboardHandler)
    if open_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    print(f"Local pipeline dashboard: {url}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return url


HTML = r"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Debt Pipeline Dashboard</title>
  <style>
    :root {
      --bg: #f4f1ea;
      --ink: #17211b;
      --muted: #657067;
      --line: #d8d2c5;
      --panel: #fffdf8;
      --green: #0f8a5f;
      --blue: #2456a6;
      --amber: #b36b00;
      --red: #b32d2e;
      --shadow: 0 12px 30px rgba(33, 29, 20, 0.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: linear-gradient(120deg, #f4f1ea 0%, #eef3ef 55%, #f8f3e5 100%);
      color: var(--ink);
      font: 15px/1.45 "Segoe UI", "PT Sans", sans-serif;
    }
    header {
      padding: 28px clamp(18px, 4vw, 52px) 18px;
      border-bottom: 1px solid var(--line);
      background: rgba(255, 253, 248, 0.72);
      backdrop-filter: blur(10px);
      position: sticky;
      top: 0;
      z-index: 3;
    }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 18px;
      max-width: 1360px;
      margin: 0 auto;
    }
    h1 {
      margin: 0;
      font-size: clamp(24px, 3vw, 38px);
      letter-spacing: 0;
      line-height: 1.08;
    }
    .subtitle {
      margin: 8px 0 0;
      color: var(--muted);
      max-width: 780px;
    }
    button {
      border: 0;
      background: var(--ink);
      color: #fff;
      min-height: 44px;
      padding: 0 18px;
      border-radius: 7px;
      font-weight: 700;
      cursor: pointer;
      box-shadow: var(--shadow);
      white-space: nowrap;
    }
    button:disabled {
      cursor: wait;
      opacity: 0.65;
    }
    main {
      max-width: 1360px;
      margin: 0 auto;
      padding: 26px clamp(18px, 4vw, 52px) 48px;
    }
    .upload-panel {
      display: grid;
      grid-template-columns: 0.85fr 1.15fr;
      gap: 18px;
      align-items: start;
      margin-bottom: 22px;
      background:
        radial-gradient(circle at 16% 20%, rgba(15, 138, 95, 0.12), transparent 30%),
        radial-gradient(circle at 90% 10%, rgba(36, 86, 166, 0.12), transparent 26%),
        rgba(255, 253, 248, 0.9);
      border: 1px solid var(--line);
      border-radius: 10px;
      box-shadow: var(--shadow);
      padding: 18px;
    }
    .upload-panel h2 {
      margin: 0 0 10px;
      font-size: 24px;
    }
    .upload-panel p {
      margin: 0 0 12px;
      color: var(--muted);
    }
    .upload-flow {
      display: grid;
      gap: 8px;
      margin-top: 16px;
    }
    .flow-step {
      display: grid;
      grid-template-columns: 30px 1fr;
      gap: 10px;
      align-items: start;
      color: var(--muted);
    }
    .flow-step strong {
      display: inline-grid;
      place-items: center;
      width: 30px;
      height: 30px;
      border-radius: 999px;
      color: #fff;
      background: var(--ink);
    }
    .slot-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }
    .slot-card {
      display: grid;
      gap: 7px;
      padding: 11px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: rgba(255, 255, 255, 0.68);
    }
    .slot-card.required {
      border-color: rgba(15, 138, 95, 0.45);
      background: rgba(223, 240, 233, 0.48);
    }
    .slot-card label {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      font-weight: 800;
      font-size: 13px;
    }
    .slot-card small {
      color: var(--muted);
    }
    .slot-card input {
      width: 100%;
      font-size: 13px;
    }
    .form-actions {
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
      margin-top: 12px;
    }
    .upload-message {
      color: var(--muted);
      font-weight: 700;
    }
    .upload-message.error {
      color: var(--red);
    }
    .summary {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 22px;
    }
    .metric, .stage, .artifact {
      background: rgba(255, 253, 248, 0.86);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }
    .metric { padding: 16px; }
    .metric span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    .metric strong {
      display: block;
      margin-top: 6px;
      font-size: 22px;
    }
    .section-title {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 14px;
      margin: 28px 0 12px;
    }
    .section-title h2 {
      margin: 0;
      font-size: 21px;
    }
    .section-title p {
      margin: 0;
      color: var(--muted);
    }
    .pipeline {
      display: grid;
      grid-template-columns: repeat(7, minmax(150px, 1fr));
      gap: 10px;
      overflow-x: auto;
      padding-bottom: 4px;
    }
    .stage {
      min-height: 192px;
      padding: 14px;
      position: relative;
      border-top: 5px solid var(--line);
    }
    .stage.completed { border-top-color: var(--green); }
    .stage.running { border-top-color: var(--blue); }
    .stage.failed { border-top-color: var(--red); }
    .stage h3 {
      margin: 0 0 8px;
      font-size: 16px;
    }
    .stage p {
      margin: 0;
      color: var(--muted);
      min-height: 62px;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      margin-top: 12px;
      padding: 5px 8px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      background: #ece7db;
      color: var(--muted);
    }
    .badge.completed { background: #dff0e9; color: var(--green); }
    .badge.running { background: #dfe8fa; color: var(--blue); }
    .badge.failed { background: #f5dddd; color: var(--red); }
    .duration {
      color: var(--muted);
      font-size: 13px;
      margin-top: 10px;
    }
    .grid {
      display: grid;
      grid-template-columns: 1.1fr 0.9fr;
      gap: 18px;
      align-items: start;
    }
    .decision-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }
    .panel {
      background: rgba(255, 253, 248, 0.9);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      padding: 16px;
    }
    .panel h3 {
      margin: 0 0 10px;
      font-size: 17px;
    }
    .panel p {
      margin: 0;
      color: var(--muted);
    }
    .profile-list {
      display: grid;
      gap: 12px;
    }
    .profile-card {
      background: rgba(255, 253, 248, 0.92);
      border: 1px solid var(--line);
      border-left: 5px solid var(--blue);
      border-radius: 8px;
      box-shadow: var(--shadow);
      padding: 16px;
    }
    .profile-head {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
      margin-bottom: 10px;
    }
    .profile-title {
      margin: 0;
      font-size: 18px;
    }
    .profile-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 10px 0;
    }
    .pill {
      display: inline-flex;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--muted);
      border-radius: 999px;
      padding: 5px 9px;
      font-size: 12px;
      font-weight: 700;
    }
    .action-pill {
      color: #fff;
      background: var(--green);
      border-color: var(--green);
    }
    .no-action {
      color: var(--ink);
      background: #ece7db;
      border-color: #ece7db;
    }
    .profile-body {
      display: grid;
      grid-template-columns: 1.1fr 0.9fr;
      gap: 14px;
      margin-top: 10px;
    }
    .profile-body p {
      margin: 0;
      color: var(--muted);
    }
    .mini-metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
      margin-top: 12px;
    }
    .mini {
      border-top: 1px solid var(--line);
      padding-top: 8px;
    }
    .mini span {
      display: block;
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
    }
    .mini strong {
      display: block;
      margin-top: 2px;
      font-size: 15px;
    }
    .model-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    .model-table th, .model-table td {
      padding: 9px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    .model-table th {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
    }
    .artifact {
      padding: 16px;
      overflow: hidden;
    }
    .artifact h3 {
      margin: 0 0 12px;
      font-size: 17px;
    }
    .table-list {
      display: grid;
      gap: 8px;
      max-height: 430px;
      overflow: auto;
      padding-right: 4px;
    }
    .row {
      display: grid;
      grid-template-columns: minmax(120px, 1fr) 90px 90px 64px;
      gap: 10px;
      padding: 9px 0;
      border-bottom: 1px solid var(--line);
      align-items: center;
    }
    .row a {
      color: var(--blue);
      text-decoration: none;
      font-weight: 700;
      overflow-wrap: anywhere;
    }
    .row span {
      color: var(--muted);
      font-size: 13px;
    }
    .empty {
      color: var(--muted);
      padding: 12px 0;
    }
    .error {
      color: var(--red);
      font-weight: 700;
      overflow-wrap: anywhere;
    }
    @media (max-width: 960px) {
      .topbar, .grid, .upload-panel { display: block; }
      button { margin-top: 14px; width: 100%; }
      .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .decision-grid { grid-template-columns: 1fr; }
      .profile-body { grid-template-columns: 1fr; }
      .mini-metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .slot-grid { grid-template-columns: 1fr; }
      .pipeline { grid-template-columns: repeat(7, 180px); }
    }
    @media (max-width: 560px) {
      .summary { grid-template-columns: 1fr; }
      .mini-metrics { grid-template-columns: 1fr; }
      .row { grid-template-columns: 1fr 72px; }
      .row span:nth-child(2), .row span:nth-child(3) { display: none; }
    }
  </style>
</head>
<body>
  <header>
    <div class="topbar">
      <div>
        <h1>Debt Decision Console</h1>
        <p class="subtitle">Админ-панель для скоринга портфеля: профили должников, рекомендуемые меры на ближайший месяц и понятное обоснование решений.</p>
      </div>
      <button id="runBtn" type="button">Запустить пайплайн</button>
    </div>
  </header>
  <main>
    <section class="upload-panel">
      <div>
        <h2>Новый расчет из выгрузок</h2>
        <p>Загрузите стандартные файлы из учетной системы. Система сама соберет расчетную витрину, выделит профили должников и выдаст рекомендации по мерам воздействия.</p>
        <div class="upload-flow">
          <div class="flow-step"><strong>1</strong><span>Берем эталонный пакет `01-14` как базу и константы модели.</span></div>
          <div class="flow-step"><strong>2</strong><span>Подменяем загруженные пользователем выгрузки в нужных слотах.</span></div>
          <div class="flow-step"><strong>3</strong><span>Запускаем полный pipeline и показываем результат выбранного запуска.</span></div>
        </div>
      </div>
      <form id="uploadForm">
        <div class="slot-grid">
          <div class="slot-card">
            <label>01 Профиль ЛС <small>опц.</small></label>
            <input type="file" name="source_01" accept=".xlsx">
          </div>
          <div class="slot-card required">
            <label>02 Баланс / ОСВ <small>обяз.</small></label>
            <input type="file" name="source_02" accept=".xlsx" required>
          </div>
          <div class="slot-card required">
            <label>03 Оплаты <small>обяз.</small></label>
            <input type="file" name="source_03" accept=".csv" required>
          </div>
          <div class="slot-card">
            <label>04 Автодозвон <small>рек.</small></label>
            <input type="file" name="source_04" accept=".xlsx">
          </div>
          <div class="slot-card">
            <label>05 E-mail <small>рек.</small></label>
            <input type="file" name="source_05" accept=".xlsx">
          </div>
          <div class="slot-card">
            <label>06 СМС <small>рек.</small></label>
            <input type="file" name="source_06" accept=".xlsx">
          </div>
          <div class="slot-card">
            <label>07 Обзвон оператором <small>рек.</small></label>
            <input type="file" name="source_07" accept=".xlsx">
          </div>
          <div class="slot-card">
            <label>08 Претензия <small>рек.</small></label>
            <input type="file" name="source_08" accept=".xlsx">
          </div>
          <div class="slot-card">
            <label>09 Выезд к абоненту <small>рек.</small></label>
            <input type="file" name="source_09" accept=".xlsx">
          </div>
          <div class="slot-card">
            <label>10 Уведомление об ограничении <small>рек.</small></label>
            <input type="file" name="source_10" accept=".xlsx">
          </div>
          <div class="slot-card">
            <label>11 Ограничение <small>рек.</small></label>
            <input type="file" name="source_11" accept=".xlsx">
          </div>
          <div class="slot-card">
            <label>12 Заявление о судебном приказе <small>рек.</small></label>
            <input type="file" name="source_12" accept=".xlsx">
          </div>
          <div class="slot-card">
            <label>13 Судебный приказ / ИЛ <small>рек.</small></label>
            <input type="file" name="source_13" accept=".xlsx">
          </div>
          <div class="slot-card">
            <label>14 Лимиты мер <small>опц.</small></label>
            <input type="file" name="source_14" accept=".xlsx">
          </div>
        </div>
        <div class="form-actions">
          <span class="upload-message" id="uploadMessage">Обязательные файлы: 02 и 03. Остальное можно оставить из базового пакета.</span>
          <button id="uploadBtn" type="submit">Загрузить и рассчитать</button>
        </div>
      </form>
    </section>

    <section class="summary" id="summary"></section>

    <div class="section-title">
      <h2>Вывод модели</h2>
      <p id="stateText">Загрузка статуса...</p>
    </div>
    <section class="decision-grid" id="decisionOverview"></section>

    <div class="section-title">
      <h2>Сегментация портфеля</h2>
      <p>Типовые профили должников и бизнес-гейты</p>
    </div>
    <section class="decision-grid" id="gateGrid"></section>

    <div class="section-title">
      <h2>Рекомендации по профилям</h2>
      <p>Оптимальная мера, обоснование и ожидаемый эффект</p>
    </div>
    <section class="profile-list" id="profiles"></section>

    <div class="section-title">
      <h2>Качество и рациональность мер</h2>
      <p>Uplift-модель, бизнес-ограничения и риск лишнего воздействия</p>
    </div>
    <section class="panel">
      <div id="modelQuality"></div>
    </section>

    <div class="section-title">
      <h2>Этапы</h2>
      <p>Технический статус последнего расчета</p>
    </div>
    <section class="pipeline" id="pipeline"></section>
    <p class="error" id="errorText"></p>

    <div class="section-title">
      <h2>Таблицы для проверки</h2>
      <p>Сегменты, рекомендации и исходные расчетные таблицы</p>
    </div>
    <section class="artifact">
      <div class="table-list" id="tables"></div>
    </section>
  </main>
  <script>
    const runBtn = document.getElementById("runBtn");
    const uploadForm = document.getElementById("uploadForm");
    const uploadBtn = document.getElementById("uploadBtn");
    const uploadMessage = document.getElementById("uploadMessage");
    const summary = document.getElementById("summary");
    const pipeline = document.getElementById("pipeline");
    const tables = document.getElementById("tables");
    const decisionOverview = document.getElementById("decisionOverview");
    const gateGrid = document.getElementById("gateGrid");
    const profiles = document.getElementById("profiles");
    const modelQuality = document.getElementById("modelQuality");
    const stateText = document.getElementById("stateText");
    const errorText = document.getElementById("errorText");
    let currentRunId = null;

    const labels = {
      not_started: "не запускался",
      running: "в работе",
      completed: "готово",
      failed: "ошибка"
    };

    function fmtRows(value) {
      if (value === null || value === undefined) return "n/a";
      return Number(value).toLocaleString("ru-RU");
    }

    function fmtMoney(value) {
      if (value === null || value === undefined) return "n/a";
      return `${Number(value).toLocaleString("ru-RU", { maximumFractionDigits: 0 })} ₽`;
    }

    function fmtPct(value) {
      if (value === null || value === undefined) return "n/a";
      return `${Number(value).toLocaleString("ru-RU", { maximumFractionDigits: 2 })}%`;
    }

    function renderSummary(data) {
      const decision = data.decision || {};
      const overview = decision.overview || {};
      const visibleStages = data.stages.filter(stage => !["build_presentation_viz", "build_presentation_notes"].includes(stage.name));
      const completed = visibleStages.filter(s => s.status === "completed").length;
      const month = decision.score_month || data.month || "auto";
      const runLabel = data.run_id && data.run_id !== "default" ? data.run_id : "Демо";
      summary.innerHTML = `
        <div class="metric"><span>Статус</span><strong>${data.running ? "В работе" : "Готов к запуску"}</strong></div>
        <div class="metric"><span>Запуск</span><strong>${runLabel}</strong></div>
        <div class="metric"><span>Период</span><strong>${month}</strong></div>
        <div class="metric"><span>ЛС в скоринге</span><strong>${fmtRows(overview.total_accounts || 0)}</strong></div>
      `;
      stateText.textContent = data.running ? "Расчет выполняется, рекомендации обновятся автоматически" : `Расчет завершен: ${completed}/${visibleStages.length} этапов`;
      errorText.textContent = data.last_error || "";
      runBtn.disabled = data.running;
      uploadBtn.disabled = data.running;
      runBtn.textContent = data.running ? "Расчет выполняется" : "Пересчитать рекомендации";
    }

    function renderDecision(data) {
      const decision = data.decision || {};
      const overview = decision.overview || {};
      const mix = decision.recommendation_mix || [];
      const topAction = mix.find(item => item.measure !== "no_action") || {};
      decisionOverview.innerHTML = `
        <article class="panel">
          <h3>Решение на ближайший месяц</h3>
          <p>${fmtRows(overview.positive_actions || 0)} ЛС получили активную меру. ${fmtRows(overview.no_action || 0)} ЛС оставлены без активного воздействия.</p>
          <div class="profile-meta">
            <span class="pill action-pill">${fmtPct(overview.positive_actions_share_pct || 0)} активных мер</span>
            <span class="pill no-action">${fmtPct(overview.no_action_share_pct || 0)} no action</span>
          </div>
        </article>
        <article class="panel">
          <h3>Ожидаемый эффект</h3>
          <p>Суммарный прогнозный прирост: около ${fmtRows(overview.expected_incremental_payers || 0)} вероятностных оплат в следующем месяце.</p>
          <div class="profile-meta">
            <span class="pill">Горизонт: T+1</span>
            <span class="pill">Цель: факт оплаты</span>
          </div>
        </article>
        <article class="panel">
          <h3>Главная активная мера</h3>
          <p>${topAction.label || "Активная мера не выбрана"}${topAction.accounts ? `: ${fmtRows(topAction.accounts)} ЛС` : ""}</p>
          <div class="profile-meta">
            <span class="pill">${fmtRows(overview.rule_blocked || 0)} rule-only</span>
            <span class="pill">${fmtRows(overview.non_positive_uplift || 0)} без положительного uplift</span>
          </div>
        </article>
      `;
    }

    function renderGates(gates) {
      if (!gates || !gates.length) {
        gateGrid.innerHTML = `<div class="empty">Сегменты появятся после расчета.</div>`;
        return;
      }
      gateGrid.innerHTML = gates.map(gate => `
        <article class="panel">
          <h3>${gate.label}</h3>
          <p>${gate.rationale}</p>
          <div class="profile-meta">
            <span class="pill">${fmtRows(gate.accounts)} ЛС</span>
            <span class="pill">${fmtPct(gate.share_pct)} портфеля</span>
            <span class="pill">${fmtPct(gate.ml_eligible_share_pct)} ML-eligible</span>
          </div>
        </article>
      `).join("");
    }

    function renderProfiles(items) {
      if (!items || !items.length) {
        profiles.innerHTML = `<div class="empty">Профили появятся после расчета рекомендаций.</div>`;
        return;
      }
      profiles.innerHTML = items.slice(0, 12).map(item => {
        const isAction = item.top_recommendation && item.top_recommendation !== "no_action";
        return `
          <article class="profile-card">
            <div class="profile-head">
              <div>
                <h3 class="profile-title">${item.business_gate_label}</h3>
                <div class="profile-meta">
                  <span class="pill">${item.profile_label}</span>
                  <span class="pill">${fmtRows(item.accounts)} ЛС</span>
                  <span class="pill">${fmtPct(item.share_pct)} портфеля</span>
                </div>
              </div>
              <span class="pill ${isAction ? "action-pill" : "no-action"}">${item.top_recommendation_label}</span>
            </div>
            <div class="profile-body">
              <p><strong>Обоснование.</strong> ${item.rationale}</p>
              <p><strong>Экономика и риск.</strong> ${item.economic_effect} ${item.business_constraints}</p>
            </div>
            <div class="mini-metrics">
              <div class="mini"><span>Рекомендация</span><strong>${fmtPct(item.top_recommendation_share_pct)}</strong></div>
              <div class="mini"><span>Средний долг</span><strong>${fmtMoney(item.avg_debt)}</strong></div>
              <div class="mini"><span>Оплаты 3м</span><strong>${fmtMoney(item.avg_payment_3m)}</strong></div>
              <div class="mini"><span>Uplift</span><strong>${fmtPct(item.avg_positive_uplift_pct)}</strong></div>
            </div>
          </article>
        `;
      }).join("");
    }

    function renderModelQuality(items) {
      if (!items || !items.length) {
        modelQuality.innerHTML = `<div class="empty">Метрики модели появятся после uplift-этапа.</div>`;
        return;
      }
      modelQuality.innerHTML = `
        <table class="model-table">
          <thead>
            <tr>
              <th>Мера</th>
              <th>DR ATE</th>
              <th>Naive ATE</th>
              <th>Обучение</th>
              <th>Вердикт и ограничения</th>
            </tr>
          </thead>
          <tbody>
            ${items.map(item => `
              <tr>
                <td><strong>${item.measure_label}</strong></td>
                <td>${fmtPct(item.ate_dr_pct)}</td>
                <td>${fmtPct(item.naive_ate_pct)}</td>
                <td>${fmtRows(item.train_rows)} строк, treatment ${fmtPct(item.treated_share_pct)}</td>
                <td>${item.verdict}. ${item.constraints}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
    }

    function renderStages(stages) {
      const visibleStages = stages.filter(stage => !["build_presentation_viz", "build_presentation_notes"].includes(stage.name));
      pipeline.innerHTML = visibleStages.map(stage => `
        <article class="stage ${stage.status}">
          <h3>${stage.title}</h3>
          <p>${stage.description}</p>
          <span class="badge ${stage.status}">${labels[stage.status] || stage.status}</span>
          <div class="duration">${stage.duration_sec ? `${stage.duration_sec} сек.` : ""}</div>
          ${stage.error ? `<div class="error">${stage.error}</div>` : ""}
        </article>
      `).join("");
    }

    function renderTables(items) {
      const visible = (items || []).filter(item => ["segmentation", "uplift", "mart", "normalized"].includes(item.layer));
      if (!visible.length) {
        tables.innerHTML = `<div class="empty">Таблицы появятся после запуска.</div>`;
        return;
      }
      tables.innerHTML = visible.map(item => `
        <div class="row">
          <a href="${item.url}" target="_blank">${item.name}</a>
          <span>${item.layer}</span>
          <span>${fmtRows(item.rows)} строк</span>
          <span>${item.size_mb} MB</span>
        </div>
      `).join("");
    }

    async function refresh() {
      const statusUrl = currentRunId ? `/api/runs/${currentRunId}/status` : "/api/status";
      const response = await fetch(statusUrl);
      const data = await response.json();
      renderSummary(data);
      renderDecision(data);
      renderGates(data.decision.gates);
      renderProfiles(data.decision.profiles);
      renderModelQuality(data.decision.model_quality);
      renderStages(data.stages);
      renderTables(data.outputs.tables);
    }

    runBtn.addEventListener("click", async () => {
      currentRunId = null;
      runBtn.disabled = true;
      uploadMessage.textContent = "Запущен расчет на базовом пакете.";
      uploadMessage.classList.remove("error");
      await fetch("/api/run", { method: "POST" });
      refresh();
    });

    uploadForm.addEventListener("submit", async event => {
      event.preventDefault();
      let createdRun = false;
      uploadBtn.disabled = true;
      uploadMessage.textContent = "Файлы загружаются, создается отдельный запуск...";
      uploadMessage.classList.remove("error");
      try {
        const response = await fetch("/api/runs", {
          method: "POST",
          body: new FormData(uploadForm)
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.message || payload.error || "Не удалось создать запуск");
        }
        currentRunId = payload.run_id;
        createdRun = true;
        uploadForm.reset();
        uploadMessage.textContent = `Запуск ${payload.run_id} создан. Рекомендации обновятся после завершения расчета.`;
        await refresh();
      } catch (error) {
        uploadMessage.textContent = error.message;
        uploadMessage.classList.add("error");
      } finally {
        if (!createdRun) uploadBtn.disabled = false;
      }
    });

    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
"""
