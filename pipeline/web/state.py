"""Pipeline execution state management and run tracking."""
from __future__ import annotations

import json, re, threading, traceback
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from .. import run_pipeline
from ..settings import OUTPUT_DIR, RUNS_DIR, SOURCE_DIR
from .analytics import collect_decision_summary, collect_outputs
from .helpers import read_json


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
            {"name": s["name"], "title": s["title"], "description": s["description"],
             "status": "not_started", "duration_sec": None, "metrics": {}, "error": None}
            for s in run_pipeline.STAGES
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
            pristine_default = (
                self.run_id == "default"
                and not running
                and self._started_at is None
                and self._finished_at is None
            )
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
                "outputs": collect_outputs(self.output_dir) if not pristine_default else {"tables": []},
                "decision": collect_decision_summary(self.output_dir) if not pristine_default else _empty_decision(),
                "last_metrics": read_json(self.output_dir / "load_metrics_pipeline.json") if not pristine_default else None,
            }
        if not running and not pristine_default:
            _hydrate_from_last_metrics(payload, self.output_dir)
        return payload


# ---------------------------------------------------------------------------
# Global state singletons
# ---------------------------------------------------------------------------

STATE = PipelineState()
RUN_STATES: dict[str, PipelineState] = {"default": STATE}
RUN_STATES_LOCK = threading.RLock()
ACTIVE_RUN_ID: str | None = None


# ---------------------------------------------------------------------------
# Run management
# ---------------------------------------------------------------------------

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
    return read_json(manifest_path)


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


def list_runs() -> list[dict[str, Any]]:
    runs = [{"run_id": "default", "created_at": None, "mode": "default_full_run",
             "uploaded_slots": [], "status_url": "/api/status"}]
    if RUNS_DIR.exists():
        for manifest_path in sorted(RUNS_DIR.glob("*/run_manifest.json"), reverse=True):
            manifest = read_json(manifest_path)
            if not manifest:
                continue
            run_id = str(manifest.get("run_id", manifest_path.parent.name))
            state = get_run_state(run_id)
            snapshot = state.snapshot() if state else {}
            runs.append({
                "run_id": run_id, "created_at": manifest.get("created_at"),
                "mode": manifest.get("mode", "overlay_full_run"),
                "uploaded_slots": manifest.get("uploaded_slots", []),
                "running": snapshot.get("running", False),
                "last_error": snapshot.get("last_error"),
                "month": snapshot.get("month"),
                "status_url": f"/api/runs/{run_id}/status",
                "results_url": f"/api/runs/{run_id}/results",
            })
    return runs


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


def start_pipeline_run(state: PipelineState | None = None, overrides: Mapping[str, Any] | None = None) -> bool:
    global ACTIVE_RUN_ID
    if state is None:
        state = STATE
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


# ---------------------------------------------------------------------------
# Hydration helpers  (restore state from saved metrics on disk)
# ---------------------------------------------------------------------------

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
        metrics = read_json(stage_metric_paths[stage["name"]])
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


def _empty_decision() -> dict[str, Any]:
    return {
        "score_month": "",
        "overview": {
            "total_accounts": 0,
            "positive_actions": 0,
            "positive_actions_share_pct": 0.0,
            "no_action": 0,
            "no_action_share_pct": 0.0,
            "rule_blocked": 0,
            "rule_blocked_share_pct": 0.0,
            "non_positive_uplift": 0,
            "non_positive_uplift_share_pct": 0.0,
            "expected_incremental_payers": 0.0,
        },
        "gates": [],
        "profiles": [],
        "profile_count": 0,
        "recommendation_mix": [],
        "reason_mix": [],
        "model_quality": [],
        "cluster_mix_by_gate": {},
    }
