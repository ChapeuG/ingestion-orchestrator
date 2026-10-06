"""Cria contracts/<dataset>/<tabela>.odcs.yaml a partir do template.

    python scripts/novo_contrato.py <dataset> <tabela> [--domain <area>]
"""

from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "contracts" / "template.odcs.yaml"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("dataset")
    p.add_argument("tabela")
    p.add_argument("--domain", default="<area-de-negocio>")
    args = p.parse_args()

    dataset, tabela = args.dataset.lower(), args.tabela.lower()
    target = ROOT / "contracts" / dataset / f"{tabela}.odcs.yaml"
    if target.exists():
        raise SystemExit(f"Já existe: {target}")

    now = datetime.now(timezone.utc)
    text = (TEMPLATE.read_text(encoding="utf-8")
            .replace("<uuid>", str(uuid.uuid4()))
            .replace("<dataset>", dataset)
            .replace("<tabela>", tabela)
            .replace("<area-de-negocio>", args.domain)
            .replace('"2026-01-01T00:00:00+00:00"', f'"{now.isoformat(timespec="seconds")}"')
            .replace('"2026-01-01"', f'"{now.date().isoformat()}"'))
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")
    print(f"Criado {target.relative_to(ROOT)} — preencha os campos <...> e rode: datacontract lint {target.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
