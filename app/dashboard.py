"""
Streamlit dashboard for exploring raw measurements with vantage awareness.
Run: streamlit run app/dashboard.py
"""

from pathlib import Path
from typing import Optional, List

import pandas as pd
import plotly.express as px
import streamlit as st


RAW_PATH = Path("data/raw_measurements.csv")
SUMMARY_PATH = Path("data/domain_level_summary_enriched.csv")


@st.cache_data(show_spinner=False)
def load_raw() -> pd.DataFrame:
    """Load raw measurements; ensure a vantage column exists."""
    if not RAW_PATH.exists():
        st.error(f"Missing data file: {RAW_PATH}")
        return pd.DataFrame()
    df = pd.read_csv(RAW_PATH)
    if df.empty:
        return pd.DataFrame()
    if "vantage" not in df.columns:
        df["vantage"] = "unknown"
    return df


@st.cache_data(show_spinner=False)
def load_summary() -> pd.DataFrame:
    """Load enriched domain summary if available."""
    if not SUMMARY_PATH.exists():
        return pd.DataFrame()
    df = pd.read_csv(SUMMARY_PATH)
    return df


@st.cache_data(show_spinner=False)
def list_vantage_comparisons() -> List[Path]:
    """List available vantage comparison CSVs."""
    return sorted(Path("data").glob("vantage_comparison_*_vs_*.csv"))


def load_vantage_comparison(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
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


def ca_group_map(issuer: str) -> str:
    """Group TLS issuers into coarse CA buckets."""
    if not issuer or str(issuer).strip().lower() in {"", "no_cert"}:
        return "Gov/Other"
    lower = str(issuer).lower()
    if "google trust services" in lower:
        return "Google"
    if "let's encrypt" in lower or "lets encrypt" in lower:
        return "LetsEncrypt"
    if "digicert" in lower:
        return "DigiCert"
    if "globalsign" in lower:
        return "GlobalSign"
    if "amazon" in lower:
        return "Amazon"
    if "nic" in lower:
        return "Gov/Other"
    return "Other"


def render_tls_ca_stacked(df: pd.DataFrame) -> None:
    """Stacked CA groups by category."""
    if df.empty:
        st.info("No data for TLS CA view.")
        return
    grouped = (
        df.assign(ca_group=df["tls_issuer"].fillna("").apply(ca_group_map))
        .groupby(["category", "ca_group"])["domain"]
        .nunique()
        .reset_index()
    )
    pivot = grouped.pivot(index="category", columns="ca_group", values="domain").fillna(0)
    st.write("CA groups by category (unique domains):")
    st.dataframe(pivot.astype(int))
    fig = px.bar(
        pivot,
        title="TLS CA ecosystem by category",
        labels={"value": "Number of domains", "category": "Category"},
    )
    fig.update_layout(barmode="stack")
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


def simplify_outcome(x: object) -> str:
    return "success" if str(x).lower() == "success" else "fail"


def render_vantage_diff(comp_df: pd.DataFrame) -> None:
    """Show vantage difference crosstab and bar chart."""
    if comp_df.empty:
        st.info("No comparison data available.")
        return
    comp_df["local_simple"] = comp_df["local_http_outcome"].apply(simplify_outcome)
    comp_df["remote_simple"] = comp_df["remote_http_outcome"].apply(simplify_outcome)
    ctab = pd.crosstab(comp_df["local_simple"], comp_df["remote_simple"])
    st.write("HTTP outcome crosstab (local=IN-home, remote=VPN-UK):")
    st.dataframe(ctab)
    combos = [
        ("IN-home=success / VPN-UK=success", "success", "success"),
        ("IN-home=fail / VPN-UK=success", "fail", "success"),
        ("IN-home=success / VPN-UK=fail", "success", "fail"),
        ("IN-home=fail / VPN-UK=fail", "fail", "fail"),
    ]
    records = []
    for label, lval, rval in combos:
        records.append(
            {
                "combo": label,
                "count": ctab.get(rval, pd.Series()).get(lval, 0),
            }
        )
    fig = px.bar(records, x="combo", y="count", title="HTTP outcome differences: IN-home vs VPN-UK")
    fig.update_layout(xaxis_title="Outcome combination", yaxis_title="Number of domains")
    fig.update_xaxes(tickangle=25)
    st.plotly_chart(fig, use_container_width=True)


def render_geoblocking(comp_df: pd.DataFrame) -> None:
    """Show geoblocking candidate breakdown."""
    if comp_df.empty:
        st.info("No comparison data available.")
        return
    comp_df["local_simple"] = comp_df["local_http_outcome"].apply(simplify_outcome)
    comp_df["remote_simple"] = comp_df["remote_http_outcome"].apply(simplify_outcome)
    comp_df["geoblocking_candidate"] = (comp_df["local_simple"] == "fail") & (
        comp_df["remote_simple"] == "success"
    )
    candidates = comp_df[comp_df["geoblocking_candidate"]]
    if not candidates.empty:
        st.write("Geoblocking candidates (IN-home fail, VPN-UK success):")
        st.dataframe(
            candidates[["domain", "category", "local_http_outcome", "remote_http_outcome"]],
            use_container_width=True,
        )
    grp = (
        comp_df.groupby(["category", "geoblocking_candidate"])["domain"]
        .nunique()
        .reset_index(name="count")
    )
    pivot = grp.pivot(index="category", columns="geoblocking_candidate", values="count").fillna(0)
    for col in [True, False]:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot = pivot.rename(columns={True: "geoblocking_candidate", False: "not_geoblocked"})
    st.write("Category breakdown (unique domains):")
    st.dataframe(pivot.astype(int))
    fig = px.bar(
        pivot,
        title="Geoblocking candidates by category (IN-home vs VPN-UK)",
        labels={"value": "Number of domains", "category": "Category"},
    )
    fig.update_layout(barmode="stack")
    fig.update_xaxes(tickangle=25)
    st.plotly_chart(fig, use_container_width=True)


def render_class_view(summary_df: pd.DataFrame, categories: list) -> None:
    """Show censorship_class distribution (optional categories filter)."""
    if summary_df.empty:
        st.info("Classification summary not available.")
        return
    df = summary_df.copy()
    if categories:
        df = df[df["category"].isin(categories)]
    if df.empty:
        st.info("No summary rows for current category filter.")
        return
    counts = df["censorship_class"].value_counts().reset_index()
    counts.columns = ["censorship_class", "count"]
    st.write("censorship_class counts:")
    st.dataframe(counts)
    fig = px.bar(counts, x="censorship_class", y="count", title="Domains per censorship_class")
    fig.update_xaxes(tickangle=25)
    st.plotly_chart(fig, use_container_width=True)
    st.write("Sample domains (first 100):")
    st.dataframe(df[["domain", "category", "censorship_class"]].head(100), use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="Censorship Measurements", layout="wide")
    st.title("Censorship Measurements Dashboard")
    df = load_raw()
    summary_df = load_summary()
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

        comparison_files = list_vantage_comparisons()
        comparison_labels = [p.name for p in comparison_files]
        comp_path: Optional[Path] = None
        if comparison_files:
            comp_choice = st.selectbox("Vantage comparison file", comparison_labels, index=0)
            comp_path = comparison_files[comparison_labels.index(comp_choice)]

    filtered = filter_data(df, selected_categories, vantage_choice, use_latest, selected_run_id)

    if filtered.empty:
        st.warning("No data after applying filters.")
        st.stop()

    tab_overview, tab_tls_ca, tab_vantage, tab_geoblocking, tab_classes = st.tabs(
        ["Overview", "TLS CA", "Vantage diff", "Geoblocking", "Classes"]
    )

    with tab_overview:
        st.subheader("Key metrics")
        render_metrics(filtered)
        col1, col2 = st.columns(2)
        with col1:
            render_http_chart(filtered)
        with col2:
            render_tls_chart(filtered)
        st.subheader("Problematic domains")
        render_problem_table(filtered)

    with tab_tls_ca:
        st.subheader("TLS CA by category")
        render_tls_ca_stacked(filtered)

    with tab_vantage:
        st.subheader("Vantage HTTP outcome differences")
        if comp_path:
            comp_df = load_vantage_comparison(comp_path)
            render_vantage_diff(comp_df)
        else:
            st.info("No vantage comparison file found.")

    with tab_geoblocking:
        st.subheader("Geoblocking candidates")
        if comp_path:
            comp_df = load_vantage_comparison(comp_path)
            render_geoblocking(comp_df)
        else:
            st.info("No vantage comparison file found.")

    with tab_classes:
        st.subheader("Censorship classes (from enriched summary)")
        render_class_view(summary_df, selected_categories)


if __name__ == "__main__":
    main()
