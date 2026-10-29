"""Synthetic Partnerize feed and DynamoDB rows for the local emulator.

Nothing here is a real subscriber, tenant, or commission. Names are fictional
telcos so the dashboard has something to click through. Timestamps are
computed from the pipeline clock so the 24-hour and 24-day windows still
match after the sample is regenerated tomorrow.

The scenario, given clock ``now``:

* A feed uploaded 2 hours ago with Northwave and HarborTel conversions,
  plus three rows the joins must drop (unknown link, missing campaign,
  publisher reference with no dots).
* A Cedarlink feed uploaded 3 days ago. It stays in common commission and
  falls out of payment advice and the daily telco file.
* A Northwave feed uploaded exactly 24 hours ago. Commission keeps it
  (``> now - 24 days``). Advice and notifications drop it (``> now - 24 hours``).
* A HarborTel feed uploaded exactly 24 days ago, and a Northwave feed from
  30 days ago. Both are outside the commission window.
* One otherwise valid conversion whose ``upload-time`` is blank.
* One exact duplicate in config, campaign, and activation link, which the
  Glue staging step removes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


def _stamp(value: datetime) -> str:
    """Athena's ``REPLACE(upload-time, 'T', ' ')`` expects a ``T`` separator."""
    return value.strftime("%Y-%m-%dT%H:%M:%S")


def _conversion(
    conversion_id: str,
    publisher_reference: str,
    country: str,
    currency: str,
    conversion_time: str,
    items: list[tuple[str, float, str]],
    value: float,
    commission: float,
) -> dict:
    return {
        "conversion_data": {
            "publisher_reference": publisher_reference,
            "country": country,
            "currency": currency,
            "conversion_time": conversion_time,
            "conversion_id": conversion_id,
            "conversion_items": [
                {
                    "conversion_item_id": item_id,
                    "item_value": item_value,
                    "category": category,
                }
                for item_id, item_value, category in items
            ],
            "conversion_value": {
                "value": value,
                "publisher_commission": commission,
            },
        }
    }


def _feed(upload_time: str, conversions: list[dict], start: str, end: str) -> dict:
    """One Devflows JSON document, shaped like the Partnerize HTTP response."""
    return {
        "context": {
            "upload-time": upload_time,
            "last-successful-load": start,
        },
        "data": {
            "http_response_object": {
                "start_date_time_utc": start,
                "end_date_time_utc": end,
                "conversions": conversions,
            }
        },
    }


@dataclass(frozen=True)
class Fixture:
    """Raw inputs for one emulator run, duplicates included."""

    feeds: dict[str, dict]
    reference: dict[str, list[dict]]


def build_fixture(now: datetime) -> Fixture:
    """Build feeds and reference rows relative to ``now`` (naive UTC)."""
    recent_upload = now - timedelta(hours=2)
    mid_upload = now - timedelta(days=3)
    boundary_hour = now - timedelta(hours=24)
    boundary_day = now - timedelta(days=24)
    stale_upload = now - timedelta(days=30)
    recent_stamp = _stamp(recent_upload)
    recent_start = _stamp(recent_upload - timedelta(days=1))

    feeds = {
        "feed-recent.json": _feed(
            recent_stamp,
            [
                _conversion(
                    "conv-recent-1",
                    "tn-northwave.cp-summer.lk-1001",
                    "US",
                    "USD",
                    _stamp(now - timedelta(hours=2, minutes=30)),
                    [("item-1", 9.99, "plan"), ("item-2", 4.99, "addon")],
                    14.98,
                    3.50,
                ),
                _conversion(
                    "conv-recent-2",
                    "tn-northwave.cp-roam.lk-1002",
                    "GB",
                    "GBP",
                    _stamp(now - timedelta(hours=2, minutes=50)),
                    [("item-3", 5.00, "plan")],
                    5.00,
                    1.00,
                ),
                _conversion(
                    "conv-harbor",
                    "tn-harbortel.cp-family.lk-3001",
                    "KE",
                    "KES",
                    _stamp(now - timedelta(hours=4)),
                    [("item-4", 12.00, "plan")],
                    12.00,
                    1.20,
                ),
                _conversion(
                    "conv-orphan",
                    "tn-northwave.cp-summer.lk-missing",
                    "US",
                    "USD",
                    recent_stamp,
                    [("item-5", 1.00, "plan")],
                    1.00,
                    0.10,
                ),
                _conversion(
                    "conv-bad-ref",
                    "NODOTS",
                    "US",
                    "USD",
                    recent_stamp,
                    [("item-6", 1.00, "plan")],
                    1.00,
                    0.10,
                ),
                _conversion(
                    "conv-missing-campaign",
                    "tn-cedarlink.cp-missing.lk-2001",
                    "NG",
                    "NGN",
                    recent_stamp,
                    [("item-7", 1.00, "plan")],
                    1.00,
                    0.10,
                ),
            ],
            recent_start,
            recent_stamp,
        ),
        "feed-mid.json": _feed(
            _stamp(mid_upload),
            [
                _conversion(
                    "conv-older",
                    "tn-cedarlink.cp-cedar.lk-2001",
                    "NG",
                    "NGN",
                    _stamp(mid_upload - timedelta(hours=4)),
                    [("item-8", 20.00, "plan")],
                    20.00,
                    2.00,
                )
            ],
            _stamp(mid_upload - timedelta(days=1)),
            _stamp(mid_upload),
        ),
        "feed-boundary-hour.json": _feed(
            _stamp(boundary_hour),
            [
                _conversion(
                    "conv-boundary-hour",
                    "tn-northwave.cp-roam.lk-1002",
                    "US",
                    "USD",
                    _stamp(boundary_hour),
                    [("item-9", 1.00, "plan")],
                    1.00,
                    0.10,
                )
            ],
            _stamp(boundary_hour - timedelta(days=1)),
            _stamp(boundary_hour),
        ),
        "feed-boundary-day.json": _feed(
            _stamp(boundary_day),
            [
                _conversion(
                    "conv-boundary-day",
                    "tn-harbortel.cp-family.lk-3001",
                    "KE",
                    "KES",
                    _stamp(boundary_day),
                    [("item-10", 1.00, "plan")],
                    1.00,
                    0.10,
                )
            ],
            _stamp(boundary_day - timedelta(days=1)),
            _stamp(boundary_day),
        ),
        "feed-stale.json": _feed(
            _stamp(stale_upload),
            [
                _conversion(
                    "conv-stale",
                    "tn-northwave.cp-summer.lk-1001",
                    "US",
                    "USD",
                    _stamp(stale_upload),
                    [("item-11", 1.00, "plan")],
                    1.00,
                    0.10,
                )
            ],
            _stamp(stale_upload - timedelta(days=1)),
            _stamp(stale_upload),
        ),
        "feed-blank-upload.json": _feed(
            "",
            [
                _conversion(
                    "conv-blank-upload",
                    "tn-northwave.cp-summer.lk-1001",
                    "US",
                    "USD",
                    recent_stamp,
                    [("item-12", 1.00, "plan")],
                    1.00,
                    0.10,
                )
            ],
            recent_start,
            recent_stamp,
        ),
    }

    northwave = {"id": "tn-northwave", "tenantname": "Northwave Mobile"}
    summer = {"id": "cp-summer", "campaignname": "Summer Launch"}
    link_anna = {"id": "lk-1001", "offerId": "of-100", "subscriberid": "sub-anna"}
    reference = {
        "config": [
            northwave,
            {"id": "tn-cedarlink", "tenantname": "Cedarlink"},
            {"id": "tn-harbortel", "tenantname": "HarborTel"},
            # Exact duplicate. Glue's dropDuplicates drops this before the join.
            dict(northwave),
        ],
        "campaign": [
            summer,
            {"id": "cp-roam", "campaignname": "Roam Free"},
            {"id": "cp-cedar", "campaignname": "Cedar Promo"},
            {"id": "cp-family", "campaignname": "Family Bundle"},
            dict(summer),
        ],
        "offer": [
            {"id": "of-100", "name": "Unlimited Talk", "supplierid": "sup-apple"},
            {"id": "of-200", "name": "Music Pass", "supplierid": "sup-apple"},
            {"id": "of-300", "name": "Cloud Storage", "supplierid": "sup-cloud"},
        ],
        "activationlink": [
            link_anna,
            {"id": "lk-1002", "offerId": "of-200", "subscriberid": "sub-ben"},
            {"id": "lk-2001", "offerId": "of-100", "subscriberid": "sub-chi"},
            {"id": "lk-3001", "offerId": "of-300", "subscriberid": "sub-dara"},
            dict(link_anna),
        ],
    }
    return Fixture(feeds=feeds, reference=reference)
