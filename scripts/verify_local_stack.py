"""Check a finished local lake, and optionally the dashboard health endpoint.

The expected counts match the sample in ``local_emulator/sample_data.py``.
They do not depend on the wall clock, because every timestamp is generated
from the run's business clock.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
from pathlib import Path

import duckdb

EXPECTED_NOTIFICATIONS = {
    "Cedarlink": 0,
    "HarborTel": 1,
    "Northwave Mobile": 3,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lake",
        type=Path,
        default=Path(".local-lake"),
        help="Lake directory written by the emulator (default: .local-lake)",
    )
    parser.add_argument(
        "--health-url",
        default="",
        help="Optional Streamlit health URL, for example http://127.0.0.1:8501/_stcore/health",
    )
    args = parser.parse_args()
    check_lake(args.lake.resolve())
    if args.health_url:
        check_health(args.health_url)
    print(f"local stack ok ({args.lake.resolve()})")


def check_lake(lake: Path) -> None:
    manifest_path = lake / "runs" / "latest.json"
    if not manifest_path.exists():
        raise SystemExit(f"No run manifest at {manifest_path}. Run the ETL first.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "succeeded":
        raise SystemExit(f"Latest run status is {manifest.get('status')!r}, expected 'succeeded'.")
    failed = [item["name"] for item in manifest.get("quality_checks", []) if not item.get("passed")]
    if failed:
        raise SystemExit("Quality checks failed: " + ", ".join(failed))

    counts = manifest.get("counts", {})
    if counts.get("commission_rows") != 6:
        raise SystemExit(f"Expected 6 commission rows, found {counts.get('commission_rows')}.")
    if counts.get("payment_advice_rows") != 4:
        raise SystemExit(f"Expected 4 payment-advice rows, found {counts.get('payment_advice_rows')}.")
    if counts.get("notification_rows") != EXPECTED_NOTIFICATIONS:
        raise SystemExit(
            "Unexpected notification counts: "
            f"{counts.get('notification_rows')!r}"
        )

    parquet = lake / "curated" / "common-commission" / "commission.parquet"
    rows = duckdb.connect().execute(
        "SELECT count(*) FROM read_parquet(?)",
        [parquet.as_posix()],
    ).fetchall()
    if not rows or rows[0][0] != 6:
        raise SystemExit(f"Parquet row count was {rows}, expected 6.")

    advice_step = next(step for step in manifest["steps"] if step["name"] == "payment_advice")
    advice = lake / advice_step["output"]
    if not advice.exists():
        raise SystemExit(f"Payment advice CSV is missing: {advice}")


def check_health(url: str) -> None:
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            body = response.read().decode("utf-8", errors="replace")
            status = response.status
    except Exception as exc:
        raise SystemExit(f"Dashboard health check failed for {url}: {exc}") from exc
    if status != 200 or "ok" not in body.lower():
        raise SystemExit(f"Dashboard health check returned {status}: {body!r}")


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        sys.exit(0)
