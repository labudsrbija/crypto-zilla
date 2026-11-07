"""Entry point for spark-submit: cz-submit <job> [args]."""

import importlib
import sys

from cryptozilla.config import AppSettings
from cryptozilla.log import setup_logging

JOBS = {
    "bronze_trades": "cryptozilla.jobs.bronze_trades",
}


def cli() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in JOBS:
        sys.exit(f"usage: cz-submit <job> [args]\njobs: {', '.join(sorted(JOBS))}")
    app = AppSettings()
    setup_logging(app.log_level, app.log_format)
    importlib.import_module(JOBS[sys.argv[1]]).main(sys.argv[2:])


if __name__ == "__main__":
    cli()