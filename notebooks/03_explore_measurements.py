"""
Analysis script for raw connectivity measurements.
Loads the latest run from data/raw_measurements.csv, prints summaries,
writes summary CSVs, and produces a simple HTTP outcomes plot.
"""

from pathlib import Path
import sys
import pandas as pd
import matplotlib.pyplot as plt


INPUT_PATH = Path("data/raw_measurements.csv")
DATA_DIR = Path("data")
FIG_DIR = Path("figures")


def load_latest_run(input_path: Path, vantage_filter: str | None = None) -> pd.DataFrame:
    """
    Load measurements and return the latest run, optionally filtered by vantage.
    If no vantage is provided, pick the vantage+run_id combination with the latest run_id
    (vantage chosen deterministically by highest row count for that run_id).
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Missing input file: {input_path}")
    df = pd.read_csv(input_path)
    if df.empty:
        raise ValueError(f"No rows found in {input_path}")
    if "vantage" not in df.columns:
        df["vantage"] = "unknown"

    if vantage_filter:
        df_v = df[df["vantage"] == vantage_filter]
        if df_v.empty:
            raise ValueError(f"No rows found for vantage '{vantage_filter}'")
        latest_run_id = df_v["run_id"].max()
        df_run = df_v[df_v["run_id"] == latest_run_id].copy()
        if df_run.empty:
            raise ValueError(
                f"No rows found for vantage '{vantage_filter}' at latest run_id {latest_run_id}"
            )
        chosen_vantage = vantage_filter
    else:
        latest_run_id = df["run_id"].max()
        df_latest = df[df["run_id"] == latest_run_id].copy()
        if df_latest.empty:
            raise ValueError(f"No rows found for latest run_id {latest_run_id}")
        # Choose vantage with the most rows for determinism.
        chosen_vantage = df_latest["vantage"].value_counts().idxmax()
        df_run = df_latest[df_latest["vantage"] == chosen_vantage].copy()

    print(
        f"Using vantage={chosen_vantage}, run_id={latest_run_id} "
        f"with shape {df_run.shape}"
    )
    return df_run


def summarise_basic(df_run: pd.DataFrame) -> None:
    """Print basic structure info."""
    print("\n=== Basic structure ===")
    print(f"Rows: {df_run.shape[0]}, Columns: {df_run.shape[1]}")
    print("Columns:", list(df_run.columns))
    domains_per_cat = (
        df_run.groupby("category")["domain"].nunique().sort_values(ascending=False)
    )
    print("\nUnique domains per category:")
    print(domains_per_cat)


def summarise_http(df_run: pd.DataFrame) -> pd.DataFrame:
    """Summarise HTTP outcomes overall and by category; return pivot for plotting."""
    print("\n=== HTTP outcomes ===")
    vc = df_run["http_outcome"].value_counts(dropna=False)
    print("Distribution of http_outcome:")
    print(vc)

    pivot = (
        df_run.pivot_table(
            index="category",
            columns="http_outcome",
            values="domain",
            aggfunc=pd.Series.nunique,
            fill_value=0,
        )
        .sort_index()
    )
    print("\nHTTP outcomes by category (unique domains):")
    print(pivot)

    out_path = DATA_DIR / "http_outcomes_by_category.csv"
    pivot.to_csv(out_path)
    print(f"Saved HTTP outcome pivot to {out_path}")
    return pivot


def _issuer_series(series: pd.Series) -> pd.Series:
    """Normalize TLS issuer series, replacing empty/NaN with NO_CERT."""
    return series.replace("", pd.NA).fillna("NO_CERT")


def summarise_tls(df_run: pd.DataFrame) -> None:
    """Summarise TLS issuers overall and per category."""
    issuers = _issuer_series(df_run["tls_issuer"])
    top_overall = issuers.value_counts().head(15)
    print("\n=== TLS issuers overall (top 15) ===")
    print(top_overall)

    overall_path = DATA_DIR / "tls_issuers_overall.csv"
    top_overall.rename_axis("tls_issuer").reset_index(name="count").to_csv(
        overall_path, index=False
    )
    print(f"Saved overall TLS issuers to {overall_path}")

    for category, sub_df in df_run.groupby("category"):
        cat_safe = category.replace(" ", "_").replace("/", "_")
        cat_issuers = _issuer_series(sub_df["tls_issuer"])
        top_cat = cat_issuers.value_counts().head(5)
        print(f"\nTop TLS issuers for category='{category}' (top 5):")
        print(top_cat)

        cat_path = DATA_DIR / f"tls_issuers_{cat_safe}.csv"
        top_cat.rename_axis("tls_issuer").reset_index(name="count").to_csv(
            cat_path, index=False
        )
        print(f"Saved TLS issuers for {category} to {cat_path}")


def _coerce_bool(series: pd.Series) -> pd.Series:
    """Convert mixed-type boolean-like series to actual booleans."""
    return series.astype(str).str.lower().map({"true": True, "false": False}).fillna(False)


def summarise_dns(df_run: pd.DataFrame) -> None:
    """Compare local vs public DNS success and save mismatches."""
    print("\n=== DNS local vs public ===")
    local_ok = _coerce_bool(df_run["dns_local_ok"])
    public_ok = _coerce_bool(df_run["dns_public_ok"])

    frac_local = local_ok.mean()
    frac_public = public_ok.mean()
    print(f"Fraction local DNS ok: {frac_local:.3f}")
    print(f"Fraction public DNS ok: {frac_public:.3f}")

    mismatch_mask = public_ok & (~local_ok)
    mismatches = df_run.loc[mismatch_mask, ["domain", "category", "dns_local_error"]]
    print(f"Public DNS ok but local failed: {len(mismatches)} rows")
    if not mismatches.empty:
        print(mismatches.head(20).to_string(index=False))

    mismatch_path = DATA_DIR / "dns_mismatch_local_vs_public.csv"
    mismatches.to_csv(mismatch_path, index=False)
    print(f"Saved DNS mismatches to {mismatch_path}")


def plot_http_outcomes_by_category(pivot: pd.DataFrame) -> None:
    """Create and save HTTP outcomes grouped bar chart by category."""
    print("\n=== Plotting HTTP outcomes by category ===")
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    ax = pivot.plot(kind="bar", stacked=True, figsize=(12, 7))
    ax.set_title("HTTP Outcomes by Category (unique domains)")
    ax.set_xlabel("Category")
    ax.set_ylabel("Number of domains")
    ax.legend(title="http_outcome", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    fig_path = FIG_DIR / "http_outcomes_by_category.png"
    plt.savefig(fig_path, dpi=200)
    plt.close()
    print(f"Saved figure to {fig_path}")


def main(vantage_override: str | None = None) -> None:
    """Run all summaries and plot for the latest run."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    vantage_arg = (
        vantage_override
        if vantage_override is not None
        else (sys.argv[1] if len(sys.argv) > 1 else None)
    )
    df_run = load_latest_run(INPUT_PATH, vantage_filter=vantage_arg)
    summarise_basic(df_run)
    pivot = summarise_http(df_run)
    summarise_tls(df_run)
    summarise_dns(df_run)
    plot_http_outcomes_by_category(pivot)


if __name__ == "__main__":
    main()
