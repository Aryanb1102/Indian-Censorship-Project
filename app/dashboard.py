"""
Streamlit dashboard for exploring raw measurements with vantage awareness.
Run: streamlit run app/dashboard.py
"""

from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st


DATA_PATH = Path("data/raw_measurements.csv")


@st.cache_data(show_spinner=False)
def load_data() -> pd.DataFrame:
    """Load raw measurements; ensure a vantage column exists."""
    if not DATA_PATH.exists():
        st.error(f"Missing data file: {DATA_PATH}")
        return pd.DataFrame()
    df = pd.read_csv(DATA_PATH)
    if df.empty:
        return pd.DataFrame()
    if "vantage" not in df.columns:
        df["vantage"] = "unknown"
    return df


def filter_data(
    df: pd.DataFrame,
    categories: list,
    vantage: Optional[str],
    use_latest: bool,
    selected_run_id: Optional[str],
) -> pd.DataFrame:
    """Apply category, vantage, and run filters."""
    filtered = df.copy()
    if categories:
        filtered = filtered[filtered["category"].isin(categories)]
    if vantage and vantage != "ALL":
        filtered = filtered[filtered["vantage"] == vantage]
    if filtered.empty:
        return filtered

    if use_latest:
        latest_run = filtered["run_id"].max()
        filtered = filtered[filtered["run_id"] == latest_run]
    elif selected_run_id:
        filtered = filtered[filtered["run_id"] == selected_run_id]
    return filtered


def render_metrics(df: pd.DataFrame) -> None:
    """Show key metrics."""
    if df.empty:
        st.warning("No data to display with current filters.")
        return
    total_domains = df["domain"].nunique()
    success_outcomes = {"success", "redirect"}
    success_domains = df[df["http_outcome"].isin(success_outcomes)]["domain"].nunique()
    problematic_domains = total_domains - success_domains
    st.metric("Unique domains", total_domains)
    st.metric("HTTP success/redirect %", f"{(success_domains / total_domains * 100):.1f}%")
    st.metric("Non-success %", f"{(problematic_domains / total_domains * 100):.1f}%")


def render_http_chart(df: pd.DataFrame) -> None:
    """Plot HTTP outcomes counts by unique domains."""
    if df.empty:
        st.info("No data for HTTP chart.")
        return
    http_counts = (
        df.groupby("http_outcome")["domain"]
        .nunique()
        .reset_index(name="domain_count")
        .sort_values("domain_count", ascending=False)
    )
    fig = px.bar(http_counts, x="http_outcome", y="domain_count", title="HTTP outcomes")
    st.plotly_chart(fig, use_container_width=True)


def render_tls_chart(df: pd.DataFrame, top_n: int = 10) -> None:
    """Plot top TLS issuers by unique domains."""
    if df.empty:
        st.info("No data for TLS chart.")
        return
    issuers = df["tls_issuer"].replace("", pd.NA).fillna("NO_CERT")
    tls_counts = (
        df.assign(tls_issuer=issuers)
        .groupby("tls_issuer")["domain"]
        .nunique()
        .reset_index(name="domain_count")
        .sort_values("domain_count", ascending=False)
        .head(top_n)
    )
    fig = px.bar(
        tls_counts,
        x="domain_count",
        y="tls_issuer",
        orientation="h",
        title="Top TLS issuers (by unique domains)",
    )
    st.plotly_chart(fig, use_container_width=True)


def render_problem_table(df: pd.DataFrame) -> None:
    """Display problematic domains table."""
    if df.empty:
        st.info("No data to display.")
        return
    bad_mask = ~df["http_outcome"].isin(["success", "redirect"])
    bad = df.loc[
        bad_mask,
        [
            "domain",
            "category",
            "vantage",
            "http_status_code",
            "http_outcome",
            "dns_local_ok",
            "dns_public_ok",
        ],
    ].drop_duplicates()
    if bad.empty:
        st.success("No problematic domains in the current filter.")
        return
    st.write("Problematic domains (first 100):")
    st.dataframe(bad.head(100), use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="Censorship Measurements", layout="wide")
    st.title("Censorship Measurements Dashboard")
    df = load_data()
    if df.empty:
        st.stop()

    with st.sidebar:
        st.header("Filters")
        categories_all = sorted(df["category"].dropna().unique().tolist())
        selected_categories = st.multiselect(
            "Category", options=categories_all, default=categories_all
        )

        vantage_options = sorted(df["vantage"].dropna().unique().tolist())
        vantage_choice = "ALL"
        if len(vantage_options) > 1:
            vantage_choice = st.selectbox(
                "Vantage", options=["ALL"] + vantage_options, index=0
            )
        elif vantage_options:
            vantage_choice = vantage_options[0]

        use_latest = st.checkbox("Use latest run only", value=True)
        selected_run_id = None
        run_options = []
        if not use_latest:
            if vantage_choice != "ALL":
                run_options = (
                    df[df["vantage"] == vantage_choice]["run_id"]
                    .dropna()
                    .unique()
                    .tolist()
                )
            else:
                run_options = df["run_id"].dropna().unique().tolist()
            run_options = sorted(run_options, reverse=True)
            if run_options:
                selected_run_id = st.selectbox("Select run_id", run_options, index=0)
            else:
                st.warning("No run_id options available for current filters.")

    filtered = filter_data(df, selected_categories, vantage_choice, use_latest, selected_run_id)

    if filtered.empty:
        st.warning("No data after applying filters.")
        st.stop()

    st.subheader("Key metrics")
    render_metrics(filtered)

    col1, col2 = st.columns(2)
    with col1:
        render_http_chart(filtered)
    with col2:
        render_tls_chart(filtered)

    st.subheader("Problematic domains")
    render_problem_table(filtered)


if __name__ == "__main__":
    main()
