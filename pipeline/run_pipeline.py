from __future__ import annotations

import json
import sys
import time
from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import date
from pathlib import Path
from typing import Any

from .build_mart import run as run_build_mart
from .build_normalized import run as run_build_normalized
from .build_presentation_notes import run as run_build_presentation_notes
from .build_presentation_viz import run as run_build_presentation_viz
from .build_segmentation import run as run_build_segmentation
from .build_uplift_prototype import run as run_build_uplift_prototype
from .common import discover_input_layer, ensure_dir, parse_date_any, read_csv_dict
from .constants import NORMALIZED_DIRNAME
from .prepare_raw import run as run_prepare_raw
from .settings import PIPELINE_CONFIG, ROOT_DIR


ProgressCallback = Callable[[dict[str, Any]], None]

STAGES: tuple[dict[str, str], ...] = (
    {"name": "prepare_raw", "title": "Raw слой", "description": "Читает исходные Excel/CSV и приводит их к единому raw-формату."},
    {"name": "build_normalized", "title": "Нормализация", "description": "Собирает канонические таблицы и DQ-issues."},
    {"name": "build_mart", "title": "Витрина", "description": "Строит leakage-safe признаки по ЛС и месяцу."},
    {"name": "build_segmentation", "title": "Сегментация", "description": "Объединяет rule-score, KMeans и business-gates."},
    {"name": "build_uplift_prototype", "title": "Uplift", "description": "Оценивает эффект мер и выбирает рекомендацию по ЛС."},
    {"name": "build_presentation_viz", "title": "Визуализации", "description": "Готовит графики для демонстрации результатов."},
    {"name": "build_presentation_notes", "title": "Материалы", "description": "Генерирует структуру доклада, заметки и чеклист."},
)


def _deep_merge(base: Mapping[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    merged = deepcopy(dict(base))
    for key, value in incoming.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _resolve_path(value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (ROOT_DIR / path).resolve()


def _build_config(overrides: Mapping[str, Any] | None = None) -> dict[str, Any]:
    cfg = _deep_merge(PIPELINE_CONFIG, overrides or {})

    month_raw = cfg.get("month")
    month_value = None
    if month_raw is not None:
        month_text = str(month_raw).strip()
        month_value = month_text if month_text else None

    paths = {
        "input_dir": _resolve_path(cfg["input_dir"]),
        "out_dir": _resolve_path(cfg["out_dir"]),
    }
    presentation_out_raw = cfg.get("presentation", {}).get("out_dir")
    if presentation_out_raw:
        presentation_dir = _resolve_path(presentation_out_raw)
    else:
        presentation_dir = paths["out_dir"] / "presentation"
    paths["presentation_dir"] = presentation_dir

    return {
        "paths": paths,
        "month": month_value,
        "segmentation": cfg["segmentation"],
        "uplift": cfg["uplift"],
        "presentation": cfg["presentation"],
        "raw_config": cfg,
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    return value


def _detect_latest_balance_month(normalized_dir: Path) -> str:
    balance_path = normalized_dir / "fact_balance_monthly.csv"
    latest_month: date | None = None
    for row in read_csv_dict(balance_path):
        month = parse_date_any(row.get("period_month", ""))
        if month is None:
            continue
        month_start = date(month.year, month.month, 1)
        if latest_month is None or month_start > latest_month:
            latest_month = month_start

    if latest_month is None:
        raise ValueError(f"Cannot detect latest month from {balance_path}")
    return latest_month.strftime("%Y-%m")


def _emit(progress_callback: ProgressCallback | None, payload: dict[str, Any]) -> None:
    if progress_callback is not None:
        progress_callback(_json_safe(payload))


def _run_stage(name: str, fn, progress_callback: ProgressCallback | None) -> dict[str, Any]:
    started = time.perf_counter()
    _emit(progress_callback, {"stage": name, "status": "running"})
    try:
        metrics = fn()
    except Exception as exc:
        elapsed = round(time.perf_counter() - started, 3)
        _emit(progress_callback, {"stage": name, "status": "failed", "duration_sec": elapsed, "error": str(exc)})
        raise

    elapsed = round(time.perf_counter() - started, 3)
    stage_result = {
        "stage": name,
        "duration_sec": elapsed,
        "metrics": metrics,
    }
    _emit(progress_callback, {"stage": name, "status": "completed", "duration_sec": elapsed, "metrics": metrics})
    return stage_result


def run(
    overrides: Mapping[str, Any] | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    cfg = _build_config(overrides)
    paths = cfg["paths"]
    out_dir: Path = paths["out_dir"]
    presentation_dir: Path = paths["presentation_dir"]
    ensure_dir(out_dir)
    ensure_dir(presentation_dir)

    _emit(progress_callback, {"status": "pipeline_running", "stages": STAGES})

    stage_runs: list[dict[str, Any]] = []
    stage_runs.append(
        _run_stage(
            "prepare_raw",
            lambda: run_prepare_raw(paths["input_dir"], out_dir),
            progress_callback,
        )
    )

    stage_runs.append(
        _run_stage(
            "build_normalized",
            lambda: run_build_normalized(out_dir, out_dir),
            progress_callback,
        )
    )

    month = cfg["month"]
    if month is None:
        normalized_dir = discover_input_layer(out_dir, NORMALIZED_DIRNAME)
        month = _detect_latest_balance_month(normalized_dir)
        _emit(progress_callback, {"status": "month_detected", "month": month})

    seg_cfg = cfg["segmentation"]
    uplift_cfg = cfg["uplift"]
    viz_cfg = cfg["presentation"]

    stage_runs.append(
        _run_stage(
            "build_mart",
            lambda: run_build_mart(out_dir, out_dir, month),
            progress_callback,
        )
    )

    stage_runs.append(
        _run_stage(
            "build_segmentation",
            lambda: run_build_segmentation(
                input_dir=out_dir,
                out_dir=out_dir,
                segment_month=month,
                n_clusters=int(seg_cfg["k"]),
                random_state=int(seg_cfg["random_state"]),
                engine=str(seg_cfg["engine"]),
                with_hybrid=bool(seg_cfg["with_hybrid"]),
                k_hybrid_per_gate=int(seg_cfg["k_hybrid_per_gate"]),
            ),
            progress_callback,
        )
    )

    stage_runs.append(
        _run_stage(
            "build_uplift_prototype",
            lambda: run_build_uplift_prototype(
                input_dir=out_dir,
                out_dir=out_dir,
                score_month=month,
                measures_raw=uplift_cfg.get("measures"),
                max_train_rows=int(uplift_cfg["max_train_rows"]),
                random_state=int(uplift_cfg["random_state"]),
            ),
            progress_callback,
        )
    )

    stage_runs.append(
        _run_stage(
            "build_presentation_viz",
            lambda: run_build_presentation_viz(
                input_dir=out_dir,
                out_dir=presentation_dir,
                score_month=month,
                dpi=int(viz_cfg["dpi"]),
            ),
            progress_callback,
        )
    )

    stage_runs.append(
        _run_stage(
            "build_presentation_notes",
            lambda: run_build_presentation_notes(out_dir, presentation_dir),
            progress_callback,
        )
    )

    metrics: dict[str, Any] = {
        "stage": "run_pipeline",
        "input_dir": str(paths["input_dir"]),
        "out_dir": str(out_dir),
        "presentation_dir": str(presentation_dir),
        "month": month,
        "stages": stage_runs,
        "effective_config": _json_safe(cfg["raw_config"]),
    }

    metrics_path = out_dir / "load_metrics_pipeline.json"
    metrics_path.write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _emit(progress_callback, {"status": "pipeline_completed", "month": month, "metrics": metrics})
    return metrics


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    metrics = run()
    print(json.dumps(metrics, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
