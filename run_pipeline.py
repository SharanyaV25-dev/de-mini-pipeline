"""Run the four batch-pipeline stages in the required order."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
STAGES = [
    ROOT / "pipelines" / "1_ingest_raw.py",
    ROOT / "pipelines" / "2_clean_data.py",
    ROOT / "pipelines" / "3_aggregate.py",
    ROOT / "pipelines" / "4_export_json.py",
]


def main() -> None:
    for stage in STAGES:
        print(f"Running {stage.name}...")
        subprocess.run([sys.executable, str(stage)], check=True)
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
