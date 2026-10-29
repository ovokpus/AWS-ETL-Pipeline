"""Read the local lake without importing Streamlit.

The dashboard and the tests share these functions so a filter or KPI change
is checked without rendering the whole app.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

REFERENCE_TABLES = ("config", "campaign", "offer", "activationlink")

_STEP_PAYMENT = "payment_advice"
_STEP_NOTIFICATIONS = "telco_notifications"


def load_status(lake: Path) -> dict | None:
    """Return ``runs/latest.json`` or ``None`` when the lake has not been built."""
    path = lake / "runs" / "latest.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_commission(lake: Path) -> pd.DataFrame:
    path = lake / "curated" / "common-commission" / "commission.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_payment_advice(lake: Path, status: dict) -> pd.DataFrame:
    step = _step(status, _STEP_PAYMENT)
    return _read_csv(lake / step["output"])


def load_notifications(lake: Path, status: dict) -> dict[str, pd.DataFrame]:
    step = _step(status, _STEP_NOTIFICATIONS)
    return {tenant: _read_csv(lake / relative) for tenant, relative in step["outputs"].items()}


def load_reference(lake: Path, table: str) -> pd.DataFrame:
    """Read one staged reference table written by the Glue stand-in."""
    if table not in REFERENCE_TABLES:
        known = ", ".join(REFERENCE_TABLES)
        raise ValueError(f"Unknown reference table {table!r}. Expected one of: {known}")
    path = lake / "staged" / table / f"{table}.jsonl"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_json(path, lines=True)


def load_feed_summary(lake: Path) -> pd.DataFrame:
    """One row per raw Partnerize JSON file in the lake."""
    directory = lake / "raw" / "partner-feeds"
    rows: list[dict] = []
    if directory.exists():
        for path in sorted(directory.glob("*.json")):
            document = json.loads(path.read_text(encoding="utf-8"))
            conversions = document["data"]["http_response_object"]["conversions"]
            rows.append(
                {
                    "file": path.name,
                    "upload_time": document["context"]["upload-time"],
                    "conversions": len(conversions),
                    "conversion_items": sum(
                        len(conversion["conversion_data"]["conversion_items"])
                        for conversion in conversions
                    ),
                }
            )
    return pd.DataFrame(rows, columns=["file", "upload_time", "conversions", "conversion_items"])


def filter_commissions(
    frame: pd.DataFrame,
    *,
    tenants: list[str] | None = None,
    campaigns: list[str] | None = None,
    countries: list[str] | None = None,
    currencies: list[str] | None = None,
    conversion_id: str = "",
) -> pd.DataFrame:
    """Apply the commission explorer filters.

    ``None`` means the caller did not set that filter. An empty list means
    the multiselect was cleared, which matches no rows.
    """
    if frame.empty:
        return frame.copy()
    filtered = frame
    if tenants is not None:
        filtered = filtered[filtered["tenant_name"].isin(tenants)]
    if campaigns is not None:
        filtered = filtered[filtered["campaign_name"].isin(campaigns)]
    if countries is not None:
        filtered = filtered[filtered["country"].isin(countries)]
    if currencies is not None:
        filtered = filtered[filtered["currency"].isin(currencies)]
    needle = conversion_id.strip().lower()
    if needle:
        filtered = filtered[
            filtered["conversion_id"].str.lower().str.contains(needle, regex=False)
        ]
    return filtered.copy()


def commission_kpis(frame: pd.DataFrame) -> dict[str, float | int]:
    """KPIs that do not double-count conversion-level money.

    The commission grain is one row per conversion item, and publisher
    commission is stored on the conversion. Summing the item rows would
    count that commission once per item.
    """
    if frame.empty:
        return {
            "item_rows": 0,
            "conversions": 0,
            "publisher_commission": 0.0,
            "conversion_value": 0.0,
            "subscribers": 0,
        }
    unique = frame.drop_duplicates(subset=["conversion_id"])
    return {
        "item_rows": int(len(frame)),
        "conversions": int(unique["conversion_id"].nunique()),
        "publisher_commission": float(unique["publisher_commission"].sum()),
        "conversion_value": float(unique["conversion_value"].sum()),
        "subscribers": int(frame["subscriber_id"].nunique()),
    }


def commission_by_tenant(frame: pd.DataFrame) -> pd.DataFrame:
    """Publisher commission summed once per conversion, grouped by tenant."""
    columns = ["tenant_name", "publisher_commission", "conversions"]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    unique = frame.drop_duplicates(subset=["conversion_id"])
    grouped = unique.groupby("tenant_name", as_index=False).agg(
        publisher_commission=("publisher_commission", "sum"),
        conversions=("conversion_id", "nunique"),
    )
    return grouped.sort_values("publisher_commission", ascending=False)


def items_by_country(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["country", "item_rows"])
    return (
        frame.groupby("country", as_index=False)
        .size()
        .rename(columns={"size": "item_rows"})
        .sort_values("item_rows", ascending=False)
    )


def _step(status: dict, name: str) -> dict:
    for step in status.get("steps", []):
        if step.get("name") == name:
            return step
    raise KeyError(f"Run manifest has no step named {name!r}")


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)
