from __future__ import annotations

from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
SOURCE_DIR = ROOT_DIR / "data"
OUTPUT_DIR = ROOT_DIR / "output"
PRESENTATION_DIR = OUTPUT_DIR / "presentation"
RUNS_DIR = OUTPUT_DIR / "runs"

HOST = "127.0.0.1"
PORT = 8000

PIPELINE_CONFIG = {
    "input_dir": str(SOURCE_DIR),
    "out_dir": str(OUTPUT_DIR),
    "month": None,
    "segmentation": {
        "k": 8,
        "random_state": 42,
        "engine": "auto",
        "with_hybrid": True,
        "k_hybrid_per_gate": 4,
    },
    "uplift": {
        "measures": "sms,operator_call,restriction_notice",
        "max_train_rows": 250000,
        "random_state": 42,
    },
    "presentation": {
        "out_dir": str(PRESENTATION_DIR),
        "dpi": 180,
    },
}
