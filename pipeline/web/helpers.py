"""Utility functions for web interface: formatting, parsing, label mappings."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def read_json(path: Path) -> dict[str, Any] | None:
    """Read and parse a JSON file, returning None on any error."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def file_size_mb(path: Path) -> float:
    """Return file size in megabytes, rounded to 2 decimals."""
    return round(path.stat().st_size / (1024 * 1024), 2)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    """Read a CSV file into a list of dicts. Returns empty list if file missing."""
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def to_float(value: Any, default: float = 0.0) -> float:
    """Convert any value to float, returning *default* on failure."""
    text = str(value if value is not None else "").strip()
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def to_int(value: Any, default: int = 0) -> int:
    """Convert any value to int (via float rounding)."""
    return int(round(to_float(value, float(default))))


def pct(part: int | float, total: int | float) -> float:
    """Calculate percentage, returning 0 when total <= 0."""
    total_value = float(total)
    if total_value <= 0:
        return 0.0
    return round(100.0 * float(part) / total_value, 2)


def file_signature(paths: list[Path]) -> tuple[tuple[str, int, int], ...]:
    """Build a cache-signature tuple from a list of file paths."""
    signature = []
    for path in paths:
        if path.exists():
            stat = path.stat()
            signature.append((str(path), stat.st_mtime_ns, stat.st_size))
        else:
            signature.append((str(path), 0, 0))
    return tuple(signature)


# ---------------------------------------------------------------------------
# Human-readable labels
# ---------------------------------------------------------------------------

_GATE_LABELS = {
    "gate_closed_no_debt": "Нет активного долга",
    "gate_soft_monitor": "Мягкий мониторинг",
    "gate_standard_collect": "Стандартное взыскание",
    "gate_restriction_eligible": "Доступно ограничение",
    "gate_prelegal_priority": "Предюридический приоритет",
    "gate_contact_deficit": "Дефицит контактов",
}

_MEASURE_LABELS = {
    "sms": "СМС",
    "operator_call": "Звонок оператора",
    "restriction_notice": "Уведомление об ограничении",
    "no_action": "Без активной меры",
    "": "Без активной меры",
}

_REASON_LABELS = {
    "gate_ml_ineligible": "закрыто бизнес-правилом",
    "non_positive_uplift": "модель не видит положительного эффекта",
    "max_uplift": "лучший положительный uplift",
}

_GATE_RATIONALES = {
    "gate_closed_no_debt": "Активное взыскание нерационально: долга нет или он неположительный, поэтому мера несет только операционные и юридические риски.",
    "gate_soft_monitor": "Небольшой долг и недавние оплаты лучше вести мягко: агрессивная мера может стоить дороже ожидаемого возврата.",
    "gate_standard_collect": "Базовый сценарий: долг есть, жестких ограничений нет, поэтому решение выбирается по ожидаемому эффекту модели.",
    "gate_restriction_eligible": "Есть техническая возможность ограничения, поэтому уведомление допустимо, но выбирается только при положительном uplift.",
    "gate_prelegal_priority": "Высокий долг без свежих оплат требует приоритизации: здесь важны скорость реакции и юридическая корректность.",
    "gate_contact_deficit": "Контактных каналов мало, поэтому выбор меры должен учитывать риск недоставки и лишней операционной нагрузки.",
}

_MEASURE_CONSTRAINTS = {
    "sms": "Низкая стоимость и низкий юридический риск, но эффект ограничен качеством контактных данных.",
    "operator_call": "Средняя стоимость и ограниченная емкость операторов; рационально применять там, где uplift покрывает ручную нагрузку.",
    "restriction_notice": "Более высокий юридический и репутационный риск; рационально только для допустимых профилей с ожидаемым эффектом.",
    "no_action": "Экономит бюджет мер и снижает риск лишнего воздействия, когда модель или бизнес-правила не поддерживают активное действие.",
    "": "Экономит бюджет мер и снижает риск лишнего воздействия, когда модель или бизнес-правила не поддерживают активное действие.",
}


def pretty_gate(raw: str) -> str:
    return _GATE_LABELS.get(raw, raw.replace("gate_", "").replace("_", " ").title())


def pretty_measure(raw: str) -> str:
    return _MEASURE_LABELS.get(raw, raw.replace("_", " ").title())


def pretty_reason(raw: str) -> str:
    return _REASON_LABELS.get(raw, raw.replace("_", " "))


def gate_rationale(gate: str) -> str:
    return _GATE_RATIONALES.get(gate, "Профиль обрабатывается по общим правилам портфеля и прогнозному эффекту модели.")


def measure_constraints(measure: str) -> str:
    return _MEASURE_CONSTRAINTS.get(measure, "Мера оценивается по прогнозному эффекту, стоимости применения и допустимости для профиля.")


def decision_rationale(
    gate: str,
    recommendation: str,
    reason: str,
    avg_uplift: float,
    incremental_payers: float,
    avg_debt: float,
) -> tuple[str, str]:
    """Build a pair (rationale_text, economic_effect_text) for a profile card."""
    measure_label = pretty_measure(recommendation)
    if reason == "gate_ml_ineligible":
        rationale = f"{gate_rationale(gate)} Рекомендация: {measure_label.lower()}."
    elif reason == "non_positive_uplift":
        rationale = (
            "Активная мера не назначается, потому что лучший прогнозный uplift неположительный. "
            "Рациональнее не тратить бюджет воздействия и не создавать лишний контакт с абонентом."
        )
    elif recommendation and recommendation != "no_action":
        rationale = (
            f"Выбрана мера `{measure_label}`: внутри профиля она дает лучший положительный прогнозный uplift. "
            f"{measure_constraints(recommendation)}"
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
