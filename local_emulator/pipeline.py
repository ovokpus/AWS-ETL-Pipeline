"""Run the local ETL: stage reference data, then query it with DuckDB.

Each step maps onto a file already in this repo:

* ``glue-scripts/*.py`` export a DynamoDB table, drop duplicate rows, and
  write JSON. :func:`_stage_reference` does that onto the local lake.
* ``lambda-functions/common-commission.py`` inserts the enriched commission
  grain. Here that statement rebuilds ``curated/common-commission``.
* ``lambda-functions/payment-advice.py`` writes one daily CSV.
* ``lambda-functions/telco-notification.py`` writes one CSV per tenant.

The production commission Lambda ``INSERT``s a rolling 24-day window, so a
second day can repeat rows that are still inside that window. The local run
replaces the curated files instead. That keeps ``docker compose up``
repeatable. See ``PRODUCTIONIZING.md`` for the partition-overwrite fix.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd

from local_emulator.sample_data import build_fixture
from local_emulator.sql import (
    NOTIFICATION_SELECT,
    PAYMENT_ADVICE_SELECT,
    TENANT_LIST_SELECT,
    common_commission_select,
)

logger = logging.getLogger(__name__)

COMMISSION_DAYS = 24
OUTPUT_HOURS = 24
REFERENCE_TABLES = ("config", "campaign", "offer", "activationlink")


class PipelineQualityError(RuntimeError):
    """Raised when a finished run fails its own data-quality checks."""

    def __init__(self, checks: list[dict]) -> None:
        failed = [check["name"] for check in checks if not check["passed"]]
        super().__init__("Data-quality checks failed: " + ", ".join(failed))
        self.checks = checks


@dataclass
class RunResult:
    """In-memory summary of one local run. The lake holds the same facts."""

    run_id: str
    status: str
    clock: datetime
    commission_rows: list[dict]
    payment_advice_rows: list[dict]
    notifications: dict[str, list[dict]]
    notification_paths: dict[str, Path]
    payment_advice_path: Path
    staged_counts: dict[str, int]
    duplicates_removed: dict[str, int]
    quality_checks: list[dict]


def run_pipeline(lake_dir: Path, now: datetime | None = None) -> RunResult:
    """Rebuild the local lake from sample data and return the run summary.

    ``now`` is the business clock for the 24-day and 24-hour windows. It is
    not the wall clock. Tests pass a fixed value; the CLI uses UTC now.
    Timezone-aware values are converted to naive UTC before formatting, which
    matches the ``timestamp`` cast in the Lambda SQL.
    """
    lake = Path(lake_dir)
    clock = _naive_utc(now)
    run_id = uuid.uuid4().hex[:12]
    started_at = _wall_stamp()
    try:
        return _run(lake, clock, run_id, started_at)
    except PipelineQualityError:
        raise
    except Exception as exc:
        logger.exception("Local pipeline failed")
        _write_manifest(
            lake,
            {
                "run_id": run_id,
                "status": "failed",
                "started_at": started_at,
                "finished_at": _wall_stamp(),
                "clock": _stamp(clock),
                "error": str(exc),
                "windows": _windows(),
                "steps": [],
                "quality_checks": [],
            },
        )
        raise


def _run(lake: Path, clock: datetime, run_id: str, started_at: str) -> RunResult:
    _reset_outputs(lake)
    fixture = build_fixture(clock)
    _write_feeds(lake, fixture.feeds)
    staged_counts, duplicates_removed = _stage_reference(lake, fixture.reference)

    commission_cutoff = clock - timedelta(days=COMMISSION_DAYS)
    hour_cutoff = clock - timedelta(hours=OUTPUT_HOURS)
    feed_glob = _sql_path(lake / "raw" / "partner-feeds" / "*.json")
    staged = {name: _sql_path(lake / "staged" / name / f"{name}.jsonl") for name in REFERENCE_TABLES}

    connection = duckdb.connect(database=":memory:")
    try:
        connection.execute(
            """
            CREATE TABLE pipeline_clock AS
            SELECT ?::TIMESTAMP AS commission_cutoff, ?::TIMESTAMP AS hour_cutoff
            """,
            [commission_cutoff, hour_cutoff],
        )
        select_sql = common_commission_select(
            feed_glob,
            staged["config"],
            staged["campaign"],
            staged["offer"],
            staged["activationlink"],
        )
        connection.execute(f"CREATE TABLE common_commission AS {select_sql}")
        commission_path = lake / "curated" / "common-commission" / "commission.parquet"
        commission_path.parent.mkdir(parents=True, exist_ok=True)
        connection.execute(
            f"COPY common_commission TO '{_sql_path(commission_path)}' (FORMAT PARQUET)"
        )
        commission_rows = _records(connection.execute("SELECT * FROM common_commission").fetchdf())

        advice_path = (
            lake
            / "outputs"
            / "payment-advice"
            / f"{clock:%Y}"
            / f"{clock:%m}"
            / f"daily-advice-for-{clock:%Y-%m-%d}"
            / "advice.csv"
        )
        advice_frame = connection.execute(PAYMENT_ADVICE_SELECT).fetchdf()
        _write_csv(advice_frame, advice_path)
        advice_rows = _records(advice_frame)

        tenant_names = [
            row[0]
            for row in connection.execute(TENANT_LIST_SELECT).fetchall()
        ]
        notifications: dict[str, list[dict]] = {}
        notification_paths: dict[str, Path] = {}
        for tenant in tenant_names:
            frame = connection.execute(NOTIFICATION_SELECT, [tenant]).fetchdf()
            path = (
                lake
                / "outputs"
                / "notifications"
                / _slug(tenant)
                / f"{clock:%Y}"
                / f"{clock:%m}"
                / f"daily-notification-for-{clock:%Y-%m-%d}"
                / "notification.csv"
            )
            _write_csv(frame, path)
            notifications[tenant] = _records(frame)
            notification_paths[tenant] = path
    finally:
        connection.close()

    checks = _quality_checks(
        commission_rows,
        advice_rows,
        notifications,
        staged_counts,
        clock,
    )
    status = "succeeded" if all(check["passed"] for check in checks) else "failed"
    manifest = {
        "run_id": run_id,
        "status": status,
        "started_at": started_at,
        "finished_at": _wall_stamp(),
        "clock": _stamp(clock),
        "windows": _windows(),
        "staged_counts": staged_counts,
        "duplicates_removed": duplicates_removed,
        "counts": {
            "commission_rows": len(commission_rows),
            "payment_advice_rows": len(advice_rows),
            "notification_rows": {tenant: len(rows) for tenant, rows in notifications.items()},
        },
        "steps": [
            {
                "name": "stage_reference_tables",
                "status": "succeeded",
                "staged_counts": staged_counts,
                "duplicates_removed": duplicates_removed,
            },
            {
                "name": "build_common_commission",
                "status": "succeeded",
                "rows": len(commission_rows),
                "output": _rel(lake, commission_path),
            },
            {
                "name": "payment_advice",
                "status": "succeeded",
                "rows": len(advice_rows),
                "output": _rel(lake, advice_path),
            },
            {
                "name": "telco_notifications",
                "status": "succeeded",
                "rows_by_tenant": {tenant: len(rows) for tenant, rows in notifications.items()},
                "outputs": {tenant: _rel(lake, path) for tenant, path in notification_paths.items()},
            },
        ],
        "quality_checks": checks,
    }
    _write_manifest(lake, manifest)
    logger.info(
        "Run %s %s: %s commission rows, %s advice rows",
        run_id,
        status,
        len(commission_rows),
        len(advice_rows),
    )
    result = RunResult(
        run_id=run_id,
        status=status,
        clock=clock,
        commission_rows=commission_rows,
        payment_advice_rows=advice_rows,
        notifications=notifications,
        notification_paths=notification_paths,
        payment_advice_path=advice_path,
        staged_counts=staged_counts,
        duplicates_removed=duplicates_removed,
        quality_checks=checks,
    )
    if status != "succeeded":
        raise PipelineQualityError(checks)
    return result


def _stage_reference(
    lake: Path, reference: dict[str, list[dict]]
) -> tuple[dict[str, int], dict[str, int]]:
    """Write raw DynamoDB-style exports, then the deduped Glue JSON."""
    staged_counts: dict[str, int] = {}
    duplicates_removed: dict[str, int] = {}
    for name in REFERENCE_TABLES:
        rows = reference[name]
        unique_rows = _drop_duplicates(rows)
        _write_ndjson(lake / "raw" / "dynamodb" / f"{name}.jsonl", rows)
        _write_ndjson(lake / "staged" / name / f"{name}.jsonl", unique_rows)
        staged_counts[name] = len(unique_rows)
        duplicates_removed[name] = len(rows) - len(unique_rows)
    return staged_counts, duplicates_removed


def _write_feeds(lake: Path, feeds: dict[str, dict]) -> None:
    destination = lake / "raw" / "partner-feeds"
    destination.mkdir(parents=True, exist_ok=True)
    for name, document in feeds.items():
        path = destination / name
        path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def _drop_duplicates(rows: list[dict]) -> list[dict]:
    """Same rule as the Glue scripts: ``dropDuplicates()`` on the whole row."""
    seen: set[str] = set()
    unique: list[dict] = []
    for row in rows:
        key = json.dumps(row, sort_keys=True, separators=(",", ":"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def _quality_checks(
    commission_rows: list[dict],
    advice_rows: list[dict],
    notifications: dict[str, list[dict]],
    staged_counts: dict[str, int],
    clock: datetime,
) -> list[dict]:
    """Cheap gates that mirror the rules called out in PRODUCTIONIZING.md."""
    checks: list[dict] = []

    def add(name: str, passed: bool, detail: str) -> None:
        checks.append({"name": name, "passed": bool(passed), "detail": detail})

    add("commission_has_rows", len(commission_rows) > 0, f"{len(commission_rows)} rows")

    required = ("supplier", "tenant_name", "subscriber_id", "conversion_id", "load_time")
    empty_required = 0
    for row in commission_rows:
        for column in required:
            if row.get(column) in (None, ""):
                empty_required += 1
    add(
        "required_fields_present",
        empty_required == 0 and len(commission_rows) > 0,
        f"{empty_required} empty required values",
    )

    advice_ids = {row["conversion_id"] for row in advice_rows}
    commission_ids = {row["conversion_id"] for row in commission_rows}
    add(
        "advice_rows_come_from_commission",
        advice_ids <= commission_ids,
        f"{len(advice_ids - commission_ids)} advice ids missing from commission",
    )

    commission_cutoff = clock - timedelta(days=COMMISSION_DAYS)
    hour_cutoff = clock - timedelta(hours=OUTPUT_HOURS)
    stale_commission = [
        row["conversion_id"]
        for row in commission_rows
        if _parse_stamp(row["load_time"]) <= commission_cutoff
    ]
    add(
        "commission_respects_24_day_window",
        not stale_commission,
        "ok" if not stale_commission else ", ".join(sorted(set(stale_commission))),
    )
    stale_advice = [
        row["conversion_id"]
        for row in advice_rows
        if _parse_stamp(row["load_time"]) <= hour_cutoff
    ]
    add(
        "advice_respects_24_hour_window",
        not stale_advice,
        "ok" if not stale_advice else ", ".join(sorted(set(stale_advice))),
    )

    commission_tenants = {row["tenant_name"] for row in commission_rows}
    add(
        "notification_tenants_match_commission",
        set(notifications) == commission_tenants,
        f"notification={sorted(notifications)} commission={sorted(commission_tenants)}",
    )
    add(
        "reference_tables_nonempty",
        all(staged_counts.get(name, 0) > 0 for name in REFERENCE_TABLES),
        str(staged_counts),
    )
    return checks


def _write_manifest(lake: Path, manifest: dict) -> None:
    runs = lake / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(manifest, indent=2) + "\n"
    (runs / "latest.json").write_text(payload, encoding="utf-8")
    with (runs / "history.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(manifest, separators=(",", ":")) + "\n")


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _write_ndjson(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows)
    path.write_text(body, encoding="utf-8")


def _records(frame: pd.DataFrame) -> list[dict]:
    """Turn a DuckDB frame into plain dicts (no numpy scalars)."""
    records: list[dict] = []
    for row in frame.to_dict(orient="records"):
        plain: dict = {}
        for key, value in row.items():
            if hasattr(value, "item"):
                value = value.item()
            plain[key] = value
        records.append(plain)
    return records


def _reset_outputs(lake: Path) -> None:
    """Replace data directories. ``runs/history.jsonl`` is kept."""
    for name in ("raw", "staged", "curated", "outputs"):
        path = lake / name
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
    (lake / "runs").mkdir(parents=True, exist_ok=True)


def _windows() -> dict[str, int]:
    return {"commission_days": COMMISSION_DAYS, "output_hours": OUTPUT_HOURS}


def _naive_utc(now: datetime | None) -> datetime:
    current = now if now is not None else datetime.now(timezone.utc)
    if current.tzinfo is not None:
        current = current.astimezone(timezone.utc).replace(tzinfo=None)
    return current.replace(microsecond=0)


def _stamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S")


def _parse_stamp(value: str) -> datetime:
    return datetime.strptime(value.replace("T", " "), "%Y-%m-%d %H:%M:%S")


def _wall_stamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _slug(tenant: str) -> str:
    """Filesystem-safe tenant segment.

    The Lambda lowercases the tenant and splices it into a bucket name.
    Sample tenants contain spaces, which are not valid in that position, so
    the local path folds every non-alphanumeric run into a single hyphen.
    """
    cleaned = re.sub(r"[^a-z0-9]+", "-", tenant.lower()).strip("-")
    return cleaned or "tenant"


def _sql_path(path: Path) -> str:
    text = path.resolve().as_posix()
    if "'" in text:
        raise ValueError(f"Lake path cannot contain a single quote: {text}")
    return text


def _rel(lake: Path, path: Path) -> str:
    return path.resolve().relative_to(lake.resolve()).as_posix()
