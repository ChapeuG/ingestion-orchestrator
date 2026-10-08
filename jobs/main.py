"""Entrypoint do spark-submit (o pacote ingestion fica instalado no cluster via pip)."""

import sys

from ingestion.main import main

if __name__ == "__main__":
    sys.exit(main())
