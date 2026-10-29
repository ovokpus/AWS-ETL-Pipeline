"""Behavior contract for the local Partnerize → commission pipeline.

The fixture clock is 2026-10-29 16:00:00 (naive UTC). Windows match the
Lambda SQL: common commission keeps uploads newer than 24 days, and the
payment-advice and telco-notification queries keep uploads newer than 24
hours. Both comparisons are strict (``>``), same as Athena ``date_add``.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from dashboard.data_access import (
    commission_kpis,
    filter_commissions,
    load_commission,
    load_feed_summary,
    load_notifications,
    load_payment_advice,
    load_reference,
    load_status,
)
from local_emulator.pipeline import run_pipeline

FIXED_NOW = datetime(2026, 10, 29, 16, 0, 0)


def _ids(rows: list[dict]) -> list[str]:
    return [row["conversion_id"] for row in rows]


def test_common_commission_joins_feed_to_reference_tables(tmp_path: Path) -> None:
    result = run_pipeline(tmp_path, now=FIXED_NOW)
    ids = _ids(result.commission_rows)

    # Two conversion items on the recent Northwave subscription become two rows.
    assert ids.count("conv-recent-1") == 2
    assert "conv-recent-2" in ids
    assert "conv-harbor" in ids
    # Three days old is inside the 24-day commission window.
    assert "conv-older" in ids
    # Exactly 24 hours old is still inside the 24-day window.
    assert "conv-boundary-hour" in ids

    assert "conv-stale" not in ids
    assert "conv-boundary-day" not in ids
    assert "conv-orphan" not in ids
    assert "conv-bad-ref" not in ids
    assert "conv-missing-campaign" not in ids
    assert "conv-blank-upload" not in ids
    assert len(ids) == 6


def test_enrichment_columns_come_from_the_joined_tables(tmp_path: Path) -> None:
    result = run_pipeline(tmp_path, now=FIXED_NOW)
    northwave = [
        row for row in result.commission_rows if row["conversion_id"] == "conv-recent-1"
    ]
    assert len(northwave) == 2

    assert {row["tenant_id"] for row in northwave} == {"tn-northwave"}
    assert {row["tenant_name"] for row in northwave} == {"Northwave Mobile"}
    assert {row["campaign_id"] for row in northwave} == {"cp-summer"}
    assert {row["campaign_name"] for row in northwave} == {"Summer Launch"}
    assert {row["offer_id"] for row in northwave} == {"of-100"}
    assert {row["offer_name"] for row in northwave} == {"Unlimited Talk"}
    assert {row["supplier"] for row in northwave} == {"sup-apple"}
    assert {row["subscriber_id"] for row in northwave} == {"sub-anna"}
    assert {row["link_id"] for row in northwave} == {"lk-1001"}
    assert {row["country"] for row in northwave} == {"US"}
    assert {row["currency"] for row in northwave} == {"USD"}
    assert {row["subscription_date"] for row in northwave} == {"2026-10-29T13:30:00"}
    assert {row["load_time"] for row in northwave} == {"2026-10-29T14:00:00"}
    assert sorted(row["item_value"] for row in northwave) == pytest.approx([4.99, 9.99])
    assert northwave[0]["publisher_commission"] == pytest.approx(3.5)
    assert northwave[0]["conversion_value"] == pytest.approx(14.98)

    harbor = next(
        row for row in result.commission_rows if row["conversion_id"] == "conv-harbor"
    )
    assert harbor["tenant_name"] == "HarborTel"
    assert harbor["supplier"] == "sup-cloud"
    assert harbor["offer_name"] == "Cloud Storage"
    assert harbor["subscriber_id"] == "sub-dara"


def test_reference_tables_are_deduped_before_the_join(tmp_path: Path) -> None:
    result = run_pipeline(tmp_path, now=FIXED_NOW)

    assert result.staged_counts == {
        "config": 3,
        "campaign": 4,
        "offer": 3,
        "activationlink": 4,
    }
    assert result.duplicates_removed == {
        "config": 1,
        "campaign": 1,
        "offer": 0,
        "activationlink": 1,
    }

    config = load_reference(tmp_path, "config")
    assert sorted(config["tenantname"]) == ["Cedarlink", "HarborTel", "Northwave Mobile"]


def test_payment_advice_keeps_only_the_last_24_hours(tmp_path: Path) -> None:
    result = run_pipeline(tmp_path, now=FIXED_NOW)
    ids = _ids(result.payment_advice_rows)

    assert sorted(ids) == [
        "conv-harbor",
        "conv-recent-1",
        "conv-recent-1",
        "conv-recent-2",
    ]
    assert "conv-boundary-hour" not in ids
    assert "conv-older" not in ids

    expected = (
        Path("outputs/payment-advice/2026/10/daily-advice-for-2026-10-29/advice.csv")
    )
    assert result.payment_advice_path.relative_to(tmp_path) == expected
    assert result.payment_advice_path.exists()


def test_telco_notifications_are_split_by_tenant(tmp_path: Path) -> None:
    result = run_pipeline(tmp_path, now=FIXED_NOW)

    assert sorted(result.notifications) == ["Cedarlink", "HarborTel", "Northwave Mobile"]
    assert len(result.notifications["Northwave Mobile"]) == 3
    assert len(result.notifications["HarborTel"]) == 1
    # Cedarlink is on the commission table via an older upload, so the daily
    # file is still produced, with a header and no data rows.
    assert result.notifications["Cedarlink"] == []

    notification_columns = {
        "supplier",
        "tenant_name",
        "campaign_name",
        "offer_name",
        "country",
        "currency",
        "subscriber_id",
        "subscription_date",
    }
    assert set(result.notifications["Northwave Mobile"][0]) == notification_columns
    assert "conversion_id" not in notification_columns

    northwave_path = result.notification_paths["Northwave Mobile"]
    assert northwave_path.relative_to(tmp_path) == Path(
        "outputs/notifications/northwave-mobile/2026/10/"
        "daily-notification-for-2026-10-29/notification.csv"
    )
    header = northwave_path.read_text(encoding="utf-8").splitlines()[0]
    assert "subscriber_id" in header
    cedar_lines = result.notification_paths["Cedarlink"].read_text(encoding="utf-8").splitlines()
    assert len(cedar_lines) == 1


def test_run_writes_status_and_is_idempotent(tmp_path: Path) -> None:
    first = run_pipeline(tmp_path, now=FIXED_NOW)
    second = run_pipeline(tmp_path, now=FIXED_NOW)

    assert first.status == "succeeded"
    assert second.status == "succeeded"
    assert first.run_id != second.run_id
    assert len(second.commission_rows) == 6

    status = load_status(tmp_path)
    assert status is not None
    assert status["status"] == "succeeded"
    assert status["clock"] == "2026-10-29T16:00:00"
    assert status["windows"] == {"commission_days": 24, "output_hours": 24}
    assert all(check["passed"] for check in status["quality_checks"])

    history = (tmp_path / "runs" / "history.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(history) == 2

    # A second run replaces curated parquet instead of appending to it.
    assert len(load_commission(tmp_path)) == 6


def test_dashboard_readers_and_filters(tmp_path: Path) -> None:
    run_pipeline(tmp_path, now=FIXED_NOW)

    status = load_status(tmp_path)
    assert status is not None
    advice = load_payment_advice(tmp_path, status)
    assert sorted(advice["conversion_id"]) == [
        "conv-harbor",
        "conv-recent-1",
        "conv-recent-1",
        "conv-recent-2",
    ]

    notes = load_notifications(tmp_path, status)
    assert list(notes["Cedarlink"].columns)
    assert notes["Cedarlink"].empty
    assert len(notes["HarborTel"]) == 1

    feeds = load_feed_summary(tmp_path)
    assert "conv-blank-upload" not in set(feeds["upload_time"])
    assert (feeds["upload_time"] == "").any()
    assert int(feeds.loc[feeds["upload_time"] == "", "conversions"].sum()) == 1

    commission = load_commission(tmp_path)
    northwave = filter_commissions(commission, tenants=["Northwave Mobile"])
    assert set(northwave["tenant_name"]) == {"Northwave Mobile"}
    assert len(northwave) == 4

    one = filter_commissions(commission, conversion_id="harbor")
    assert list(one["conversion_id"]) == ["conv-harbor"]

    none = filter_commissions(
        commission, tenants=["Northwave Mobile"], countries=["KE"]
    )
    assert none.empty

    assert filter_commissions(commission, tenants=[]).empty
    assert len(filter_commissions(commission, tenants=None)) == 6

    # Commission is repeated on every item row. KPIs count it once per conversion.
    kpis = commission_kpis(commission)
    assert kpis["item_rows"] == 6
    assert kpis["conversions"] == 5
    assert kpis["publisher_commission"] == pytest.approx(7.8)
