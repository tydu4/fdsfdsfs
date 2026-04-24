"""HTTP server: request handler, routing, static file serving."""
from __future__ import annotations

import json, mimetypes, re, socket, threading, traceback, webbrowser
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from ..settings import HOST, OUTPUT_DIR, PORT
from .analytics import collect_decision_summary, collect_debtor_recommendations, collect_outputs
from .helpers import read_json
from .state import (
    STATE, ACTIVE_RUN_ID, PipelineState, RUN_STATES_LOCK,
    get_run_state, list_runs, start_pipeline_run,
)
from .uploads import (
    UploadValidationError, build_uploaded_run_package, parse_multipart_form_data,
)

STATIC_DIR = Path(__file__).resolve().parent / "static"


def _read_static(filename: str) -> bytes:
    return (STATIC_DIR / filename).read_bytes()


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

    def _send_internal_error(self, exc: Exception) -> None:
        try:
            self._send_json(
                {"error": "internal_server_error", "message": str(exc)},
                HTTPStatus.INTERNAL_SERVER_ERROR,
            )
        except Exception:
            # Connection might be already broken; nothing else to do.
            pass

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query or "")

            # --- Static files ---
            if path == "/":
                self._send_bytes(_read_static("index.html"), "text/html; charset=utf-8")
                return
            if path.startswith("/static/"):
                self._serve_static(path)
                return

            # --- API ---
            if path == "/api/status":
                self._send_json(STATE.snapshot())
                return
            if path == "/api/runs":
                self._send_json({"runs": list_runs()})
                return
            if path == "/api/debtors":
                self._send_json(_build_debtors_payload(STATE.output_dir, query))
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
                self._send_json({
                    "run_id": state.run_id,
                    "decision": collect_decision_summary(state.output_dir),
                    "outputs": collect_outputs(state.output_dir),
                    "last_metrics": read_json(state.output_dir / "load_metrics_pipeline.json"),
                })
                return
            run_debtors_match = re.match(r"^/api/runs/([^/]+)/debtors$", path)
            if run_debtors_match:
                run_id = unquote(run_debtors_match.group(1))
                try:
                    state = get_run_state(run_id)
                except KeyError:
                    state = None
                if state is None:
                    self._send_json({"error": "run_not_found"}, HTTPStatus.NOT_FOUND)
                    return
                self._send_json(_build_debtors_payload(state.output_dir, query))
                return

            if path.startswith("/outputs/"):
                self._serve_output(path)
                return

            self._send_json({"error": "not_found"}, HTTPStatus.NOT_FOUND)
        except Exception as exc:
            traceback.print_exc()
            self._send_internal_error(exc)

    def do_POST(self) -> None:
        try:
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
        except Exception as exc:
            traceback.print_exc()
            self._send_internal_error(exc)

    def _handle_create_run(self) -> None:
        from .state import ACTIVE_RUN_ID as _active_run_id

        with RUN_STATES_LOCK:
            if _active_run_id is not None:
                self._send_json({"error": "pipeline_already_running", "active_run_id": _active_run_id}, HTTPStatus.CONFLICT)
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
            files = parse_multipart_form_data(self.headers.get("Content-Type", ""), body)
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
            "presentation": {"out_dir": str(Path(manifest["output_dir"]) / "presentation")},
        }
        if not start_pipeline_run(state=state, overrides=overrides):
            self._send_json({"error": "pipeline_already_running"}, HTTPStatus.CONFLICT)
            return

        self._send_json({
            "ok": True,
            "run_id": state.run_id,
            "status_url": f"/api/runs/{state.run_id}/status",
            "results_url": f"/api/runs/{state.run_id}/results",
            "uploaded_slots": manifest["uploaded_slots"],
        }, HTTPStatus.CREATED)

    def _serve_static(self, path: str) -> None:
        rel = unquote(path.removeprefix("/static/"))
        target = (STATIC_DIR / rel).resolve()
        if STATIC_DIR.resolve() not in target.parents and target != STATIC_DIR.resolve():
            self._send_json({"error": "forbidden"}, HTTPStatus.FORBIDDEN)
            return
        if not target.exists() or not target.is_file():
            self._send_json({"error": "not_found"}, HTTPStatus.NOT_FOUND)
            return
        content_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        self._send_bytes(target.read_bytes(), content_type)

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


def _find_free_port(host: str, preferred_port: int) -> int:
    for port in range(preferred_port, preferred_port + 50):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind((host, port))
            except OSError:
                continue
            return port
    raise RuntimeError(f"No free localhost port found starting from {preferred_port}")


def _query_int(query: dict[str, list[str]], key: str, default: int) -> int:
    try:
        return int(query.get(key, [str(default)])[0])
    except (TypeError, ValueError):
        return default


def _build_debtors_payload(output_dir: Path, query: dict[str, list[str]]) -> dict[str, Any]:
    return collect_debtor_recommendations(
        output_dir=output_dir,
        offset=_query_int(query, "offset", 0),
        limit=_query_int(query, "limit", 50),
        query=query.get("q", [""])[0],
    )


def _write_server_marker(url: str, port: int) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    marker = {
        "url": url, "host": HOST, "port": port,
        "started_at": datetime.now().isoformat(timespec="seconds"),
    }
    (OUTPUT_DIR / "web_interface.json").write_text(json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")


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
