"""Entrypoint do spark-submit (o pacote ingestion vai no --py-files)."""

import sys

from ingestion.main import main

if __name__ == "__main__":
    sys.exit(main())
