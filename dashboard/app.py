"""Partner feeds desk.

A local view of the lake produced by ``python -m local_emulator``. Pages:

* Run status — the manifest written to ``runs/latest.json``
* Commissions — the common-commission parquet, one row per conversion item
* Payment advice — the 24-hour CSV
* Telco notifications — one file per tenant name
* Reference data — deduped config, campaign, offer, and activation-link tables

The lake path comes from ``LAKE_DIR``. Docker Compose sets that to ``/data/lake``.
"""

from __future__ import annotations

import sys
from pathlib import Path

# ``streamlit run dashboard/app.py`` puts the script directory on sys.path,
# not the repo root, so the package imports below would fail. Docker also
# sets PYTHONPATH=/app for the same reason.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import altair as alt
import pandas as pd
import streamlit as st

from dashboard.data_access import (
    commission_by_tenant,
    commission_kpis,
    filter_commissions,
    items_by_country,
    load_commission,
    load_feed_summary,
    load_notifications,
    load_payment_advice,
    load_reference,
    load_status,
)
from local_emulator.paths import resolve_lake_dir

BANNER = _REPO_ROOT / "assets" / "banner.jpg"
PAGES = (
    "Run status",
    "Commissions",
    "Payment advice",
    "Telco notifications",
    "Reference data",
)

# Cream text on dark copper-brown. The metric rule is the same copper as the banner.
CSS = """
<style>
.block-container { padding-top: 1.4rem; max-width: 1180px; }
div[data-testid="stMetric"] {
    background: #2a221c;
    border: 1px solid #6e4b30;
    border-left: 4px solid #e08a3c;
    border-radius: 12px;
    padding: 0.7rem 0.85rem 0.4rem;
}
div[data-testid="stMetric"] [data-testid="stMetricLabel"] p,
div[data-testid="stMetric"] label {
    color: #eadccb;
}
div[data-testid="stMetric"] [data-testid="stMetricValue"] {
    color: #fff8ef;
}
</style>
"""

COMMISSION_COLUMNS = {
    "supplier": st.column_config.TextColumn("Supplier"),
    "tenant_id": st.column_config.TextColumn("Tenant id"),
    "tenant_name": st.column_config.TextColumn("Tenant"),
    "campaign_id": st.column_config.TextColumn("Campaign id"),
    "campaign_name": st.column_config.TextColumn("Campaign"),
    "offer_id": st.column_config.TextColumn("Offer id"),
    "offer_name": st.column_config.TextColumn("Offer"),
    "country": st.column_config.TextColumn("Country"),
    "currency": st.column_config.TextColumn("Currency"),
    "subscriber_id": st.column_config.TextColumn("Subscriber"),
    "subscription_date": st.column_config.TextColumn("Subscription date"),
    "item_value": st.column_config.NumberColumn("Item value", format="%.2f"),
    "conversion_value": st.column_config.NumberColumn("Conversion value", format="%.2f"),
    "publisher_commission": st.column_config.NumberColumn("Publisher commission", format="%.2f"),
    "conversion_id": st.column_config.TextColumn("Conversion id"),
    "link_id": st.column_config.TextColumn("Link id"),
    "load_time": st.column_config.TextColumn("Load time"),
}


def main() -> None:
    st.set_page_config(
        page_title="Partner feeds desk",
        page_icon=":material/monitoring:",
        layout="wide",
    )
    st.markdown(CSS, unsafe_allow_html=True)
    lake = resolve_lake_dir()
    status = load_status(lake)
    _render_sidebar(lake, status)
    page = st.sidebar.radio("Section", PAGES, label_visibility="collapsed")
    _render_page(str(page), lake, status)


def _render_sidebar(lake: Path, status: dict | None) -> None:
    st.sidebar.markdown("## Partner feeds")
    st.sidebar.caption("Local stand-in for the Partnerize, Glue, and Athena pipeline.")
    st.sidebar.code(str(lake), language=None)
    if status is None:
        st.sidebar.warning("No run yet")
        return
    label = status.get("status", "unknown")
    if label == "succeeded":
        st.sidebar.success("Latest run succeeded")
    else:
        st.sidebar.error(f"Latest run {label}")
    st.sidebar.caption(f"Run `{status.get('run_id', '')}`")
    st.sidebar.caption(f"Business clock {status.get('clock', '')} UTC")
    if st.sidebar.button("Reload lake", width="stretch"):
        st.rerun()


def _render_page(page: str, lake: Path, status: dict | None) -> None:
    if page == "Run status":
        _render_status(lake, status)
    elif page == "Commissions":
        _render_commissions(lake, status)
    elif page == "Payment advice":
        _render_advice(lake, status)
    elif page == "Telco notifications":
        _render_notifications(lake, status)
    elif page == "Reference data":
        _render_reference(lake, status)
    else:
        raise ValueError(f"Unhandled page: {page}")


def _render_status(lake: Path, status: dict | None) -> None:
    if BANNER.exists():
        st.image(str(BANNER), width="stretch")
    st.title("Partner feeds desk")
    st.caption(
        "Common commission, daily payment advice, and per-telco notification files "
        "from the local lake. Sample tenants are fictional."
    )
    if status is None:
        st.info(
            "The local lake is empty. From the repo root run `docker compose up --build`, "
            "or `python -m local_emulator` and then `streamlit run dashboard/app.py`. "
            "Reload this page once the run finishes."
        )
        return

    if status.get("status") == "succeeded":
        st.success("Latest run succeeded")
    else:
        st.error(status.get("error") or "Latest run failed data-quality checks.")

    counts = status.get("counts", {})
    notification_rows = counts.get("notification_rows", {})
    duplicates = status.get("duplicates_removed", {})
    metric_cols = st.columns(4)
    metric_cols[0].metric("Commission rows", counts.get("commission_rows", 0))
    metric_cols[1].metric("Payment advice rows", counts.get("payment_advice_rows", 0))
    metric_cols[2].metric("Tenants notified", len(notification_rows))
    metric_cols[3].metric("Duplicates removed", sum(duplicates.values()) if duplicates else 0)

    windows = status.get("windows", {})
    st.caption(
        f"Commission keeps uploads newer than {windows.get('commission_days', 24)} days. "
        f"Payment advice and telco files keep uploads newer than {windows.get('output_hours', 24)} hours. "
        "Both cutoffs are exclusive, matching the Lambda `date_add` predicates. "
        f"Started {status.get('started_at', '')}, finished {status.get('finished_at', '')}."
    )

    left, right = st.columns((1.1, 1))
    with left:
        st.subheader("Steps")
        st.dataframe(_step_frame(status), width="stretch", hide_index=True)
    with right:
        st.subheader("Quality checks")
        checks = pd.DataFrame(status.get("quality_checks", []))
        st.dataframe(checks, width="stretch", hide_index=True)

    st.caption(f"Lake directory: `{lake}`")


def _render_commissions(lake: Path, status: dict | None) -> None:
    st.title("Common commission")
    st.caption(
        "One row per conversion item. Publisher commission is a conversion-level "
        "amount, so the totals below count each conversion once."
    )
    if not _ready(status):
        return
    frame = load_commission(lake)
    if frame.empty:
        st.info("The commission file is empty.")
        return

    tenants = _choices(frame, "tenant_name")
    campaigns = _choices(frame, "campaign_name")
    countries = _choices(frame, "country")
    currencies = _choices(frame, "currency")
    filters = st.columns(5)
    picked_tenants = filters[0].multiselect("Tenant", tenants, default=tenants)
    picked_campaigns = filters[1].multiselect("Campaign", campaigns, default=campaigns)
    picked_countries = filters[2].multiselect("Country", countries, default=countries)
    picked_currencies = filters[3].multiselect("Currency", currencies, default=currencies)
    conversion_id = filters[4].text_input("Conversion id")

    filtered = filter_commissions(
        frame,
        tenants=picked_tenants,
        campaigns=picked_campaigns,
        countries=picked_countries,
        currencies=picked_currencies,
        conversion_id=conversion_id,
    )
    kpis = commission_kpis(filtered)
    cards = st.columns(4)
    cards[0].metric("Item rows", kpis["item_rows"])
    cards[1].metric("Conversions", kpis["conversions"])
    cards[2].metric("Publisher commission", f"{kpis['publisher_commission']:,.2f}")
    cards[3].metric("Subscribers", kpis["subscribers"])

    if filtered.empty:
        st.warning("Nothing matches those filters.")
        return

    by_tenant = commission_by_tenant(filtered)
    by_country = items_by_country(filtered)
    chart_cols = st.columns(2)
    with chart_cols[0]:
        st.markdown("**Publisher commission by tenant**")
        _bar(by_tenant, "tenant_name", "publisher_commission", "Publisher commission")
    with chart_cols[1]:
        st.markdown("**Item rows by country**")
        _bar(by_country, "country", "item_rows", "Item rows")

    st.dataframe(
        filtered.sort_values(["load_time", "conversion_id"], ascending=[False, True]),
        width="stretch",
        hide_index=True,
        column_config=COMMISSION_COLUMNS,
    )
    st.download_button(
        "Download filtered CSV",
        data=filtered.to_csv(index=False).encode("utf-8"),
        file_name="common-commission.csv",
        mime="text/csv",
    )


def _render_advice(lake: Path, status: dict | None) -> None:
    st.title("Daily payment advice")
    st.caption(
        "Same columns as common commission, limited to uploads in the last 24 hours. "
        "Cedarlink's 3-day-old sample row stays on the commission table and drops off this file."
    )
    if not _ready(status):
        return
    frame = load_payment_advice(lake, status)
    output = _step_output(status, "payment_advice")
    if output:
        st.code(output, language=None)
    if frame.empty:
        st.info("The payment-advice file has a header and no rows.")
        return
    cards = st.columns(3)
    cards[0].metric("Rows", len(frame))
    cards[1].metric("Conversions", int(frame["conversion_id"].nunique()))
    cards[2].metric("Tenants", int(frame["tenant_name"].nunique()))
    st.dataframe(frame, width="stretch", hide_index=True, column_config=COMMISSION_COLUMNS)
    st.download_button(
        "Download payment advice",
        data=frame.to_csv(index=False).encode("utf-8"),
        file_name="payment-advice.csv",
        mime="text/csv",
    )


def _render_notifications(lake: Path, status: dict | None) -> None:
    st.title("Telco notifications")
    st.caption(
        "One daily file per tenant that appears in common commission. "
        "The file lists subscriber, offer, and campaign columns for the last 24 hours."
    )
    if not _ready(status):
        return
    notes = load_notifications(lake, status)
    if not notes:
        st.info("This run did not write any tenant files.")
        return
    tenant = st.selectbox("Tenant", list(notes))
    frame = notes[tenant]
    output = status_outputs(status).get(tenant, "")
    if output:
        st.code(output, language=None)
    if frame.empty:
        st.warning(
            f"{tenant} is on the commission table, but nothing was uploaded in the last "
            "24 hours. The daily file is header-only."
        )
        return
    st.metric("Rows", len(frame))
    st.dataframe(frame, width="stretch", hide_index=True)
    st.download_button(
        "Download notification",
        data=frame.to_csv(index=False).encode("utf-8"),
        file_name=f"{tenant.lower().replace(' ', '-')}-notification.csv",
        mime="text/csv",
    )


def _render_reference(lake: Path, status: dict | None) -> None:
    st.title("Reference data")
    st.caption(
        "Glue exports these DynamoDB tables to JSON and drops duplicate rows "
        "before Athena joins them to the Partnerize feed."
    )
    if not _ready(status):
        return
    removed = status.get("duplicates_removed", {})
    staged = status.get("staged_counts", {})
    cards = st.columns(4)
    for index, name in enumerate(("config", "campaign", "offer", "activationlink")):
        removed_count = int(removed.get(name, 0))
        if removed_count == 0:
            delta = "no duplicates"
        elif removed_count == 1:
            delta = "1 duplicate removed"
        else:
            delta = f"{removed_count} duplicates removed"
        cards[index].metric(name, staged.get(name, 0), delta=delta, delta_color="off")
    tabs = st.tabs(["config", "campaign", "offer", "activation link", "raw feeds"])
    for tab, name in zip(tabs[:4], ("config", "campaign", "offer", "activationlink"), strict=True):
        with tab:
            st.dataframe(load_reference(lake, name), width="stretch", hide_index=True)
    with tabs[4]:
        st.dataframe(load_feed_summary(lake), width="stretch", hide_index=True)


def _ready(status: dict | None) -> bool:
    if status is None:
        st.info(
            "The local lake is empty. Run the pipeline, then open this section again."
        )
        return False
    if status.get("status") != "succeeded":
        st.error(status.get("error") or "The latest run failed, so this section may be incomplete.")
    return True


def _choices(frame: pd.DataFrame, column: str) -> list[str]:
    return sorted(frame[column].dropna().astype(str).unique())


def _step_frame(status: dict) -> pd.DataFrame:
    rows: list[dict] = []
    for step in status.get("steps", []):
        name = step["name"]
        if name == "stage_reference_tables":
            detail = ", ".join(
                f"{table} {count}" for table, count in step.get("staged_counts", {}).items()
            )
        elif name == "telco_notifications":
            detail = ", ".join(
                f"{tenant}: {count}" for tenant, count in step.get("rows_by_tenant", {}).items()
            )
        else:
            detail = f"{step.get('rows', 0)} rows → {step.get('output', '')}"
        rows.append({"step": name, "status": step.get("status", ""), "detail": detail})
    return pd.DataFrame(rows)


def _step_output(status: dict, name: str) -> str:
    for step in status.get("steps", []):
        if step.get("name") == name:
            return str(step.get("output", ""))
    return ""


def status_outputs(status: dict) -> dict[str, str]:
    for step in status.get("steps", []):
        if step.get("name") == "telco_notifications":
            return dict(step.get("outputs", {}))
    return {}


def _bar(frame: pd.DataFrame, category: str, value: str, value_title: str) -> None:
    """Horizontal bar with cream axis text on the dark page background."""
    chart = (
        alt.Chart(frame)
        .mark_bar(color="#e08a3c", cornerRadiusEnd=4)
        .encode(
            x=alt.X(f"{value}:Q", title=value_title),
            y=alt.Y(f"{category}:N", sort="-x", title=None),
            tooltip=[alt.Tooltip(category, title=category), alt.Tooltip(value, title=value_title)],
        )
        .properties(height=max(160, 36 * len(frame)), background="transparent")
        .configure_axis(
            labelColor="#f4efe6",
            titleColor="#f4efe6",
            gridColor="#3d332c",
            domainColor="#8a5a32",
            tickColor="#8a5a32",
            labelFontSize=13,
            titleFontSize=13,
        )
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(chart, width="stretch", theme=None)


main()
