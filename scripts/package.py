"""Empacota o ingestion em dist/ingestion.zip para o spark-submit --py-files.

    python scripts/package.py
    aws s3 cp dist/ingestion.zip s3://<bucket-artefatos>/ingestion-orchestrator/
    aws s3 cp jobs/main.py          s3://<bucket-artefatos>/ingestion-orchestrator/
"""

from __future__ import annotations

import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
DIST = ROOT / "dist"


def main() -> None:
    DIST.mkdir(exist_ok=True)
    target = DIST / "ingestion.zip"
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted((SRC / "ingestion").rglob("*.py")):
            zf.write(path, path.relative_to(SRC).as_posix())
    print(f"Gerado {target}")


if __name__ == "__main__":
    main()
