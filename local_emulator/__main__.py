"""CLI entrypoint: ``python -m local_emulator``."""

from __future__ import annotations

import json
import logging

from local_emulator.paths import resolve_lake_dir
from local_emulator.pipeline import run_pipeline


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    lake = resolve_lake_dir()
    result = run_pipeline(lake)
    summary = {
        "run_id": result.run_id,
        "status": result.status,
        "lake": str(lake),
        "commission_rows": len(result.commission_rows),
        "payment_advice_rows": len(result.payment_advice_rows),
        "notifications": {tenant: len(rows) for tenant, rows in result.notifications.items()},
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
