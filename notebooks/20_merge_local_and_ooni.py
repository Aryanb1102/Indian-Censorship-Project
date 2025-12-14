"""
Merge latest local measurements (IN-home vantage) with OONI cleaned data into a
domain-level summary.

Run: python notebooks/20_merge_local_and_ooni.py
"""

from pathlib import Path
import pandas as pd

RAW_MEASUREMENTS_PATH = Path("data/raw_measurements.csv")
OONI_CLEAN_PATH = Path("data/ooni_india_webconnectivity_clean.csv")
OUTPUT_PATH = Path("data/domain_level_summary.csv")
VANTAGE_FILTER = "IN-home"


def most_common(series: pd.Series) -> str:
    """Return most frequent non-null value, else empty string."""
    vc = series.dropna().value_counts()
    return vc.idxmax() if not vc.empty else ""


def load_local_summary() -> pd.DataFrame:
    """Load latest IN-home run and compute per-domain local summaries."""
    if not RAW_MEASUREMENTS_PATH.exists():
        raise FileNotFoundError(f"Missing file: {RAW_MEASUREMENTS_PATH}")
    df = pd.read_csv(RAW_MEASUREMENTS_PATH)
    if "vantage" not in df.columns:
        raise ValueError("Expected 'vantage' column in raw measurements.")
    df_v = df[df["vantage"] == VANTAGE_FILTER]
    if df_v.empty:
        raise ValueError(f"No rows found for vantage '{VANTAGE_FILTER}'.")
    latest_run = df_v["run_id"].max()
    df_run = df_v[df_v["run_id"] == latest_run].copy()
    if df_run.empty:
        raise ValueError(f"No rows found for vantage '{VANTAGE_FILTER}' at run_id {latest_run}.")

    agg = (
        df_run.groupby(["domain", "category", "subcategory"])
        .agg(
            local_http_outcome=("http_outcome", most_common),
            local_tls_issuer=("tls_issuer", most_common),
        )
        .reset_index()
    )
    return agg


def _to_bool(val) -> bool:
    """Interpret various representations as boolean for failure/anomaly detection."""
    if isinstance(val, bool):
        return val
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return False
    s = str(val).strip().lower()
    if s in ("", "false", "0", "none", "nan"):
        return False
    return True


def load_ooni_summary() -> pd.DataFrame:
    """Load OONI cleaned data and compute per-domain failure stats."""
    if not OONI_CLEAN_PATH.exists():
        raise FileNotFoundError(f"Missing file: {OONI_CLEAN_PATH}")
    df = pd.read_csv(OONI_CLEAN_PATH)
    if df.empty:
        raise ValueError(f"No rows in {OONI_CLEAN_PATH}")

    failure_flag = df["failure"].apply(_to_bool)
    anomaly_flag = df["anomaly"].apply(_to_bool) if "anomaly" in df.columns else pd.Series(False, index=df.index)
    blocking_flag = (
        df["blocking_general"].apply(_to_bool) if "blocking_general" in df.columns else pd.Series(False, index=df.index)
    )
    df["ooni_fail_flag"] = failure_flag | anomaly_flag | blocking_flag

    agg = (
        df.groupby("domain")
        .agg(
            ooni_total_measurements=("domain", "size"),
            ooni_failure_count=("ooni_fail_flag", "sum"),
        )
        .reset_index()
    )
    agg["ooni_failure_rate"] = agg["ooni_failure_count"] / agg["ooni_total_measurements"].replace(0, pd.NA)
    agg["ooni_failure_rate"] = agg["ooni_failure_rate"].fillna(0.0)
    return agg


def merge_and_save(local_df: pd.DataFrame, ooni_df: pd.DataFrame) -> pd.DataFrame:
    """Merge local and OONI summaries, save to CSV, and return merged DataFrame."""
    merged = local_df.merge(ooni_df, on="domain", how="left")
    for col in ["ooni_total_measurements", "ooni_failure_count"]:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0).astype(int)
    if "ooni_failure_rate" in merged.columns:
        merged["ooni_failure_rate"] = merged["ooni_failure_rate"].fillna(0.0)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_PATH, index=False)
    return merged


def print_summaries(merged: pd.DataFrame) -> None:
    """Print requested summaries."""
    print(f"Merged summary shape: {merged.shape}")

    if "ooni_total_measurements" in merged.columns and "ooni_failure_rate" in merged.columns:
        subset = merged[merged["ooni_total_measurements"] > 0].copy()
        top10 = subset.sort_values("ooni_failure_rate", ascending=False).head(10)
        print("\nTop 10 domains by OONI failure rate (where total > 0):")
        print(top10[["domain", "ooni_total_measurements", "ooni_failure_count", "ooni_failure_rate"]])

        bins = [-0.001, 0, 0.2, 1.0]
        labels = ["0", "(0,0.2]", "(0.2,1]"]
        merged["ooni_rate_bin"] = pd.cut(merged["ooni_failure_rate"], bins=bins, labels=labels)
        ctab = pd.crosstab(merged["local_http_outcome"], merged["ooni_rate_bin"])
        print("\nCross-tab of local_http_outcome vs binned ooni_failure_rate:")
        print(ctab)


def main() -> None:
    local = load_local_summary()
    ooni = load_ooni_summary()
    merged = merge_and_save(local, ooni)
    print_summaries(merged)


if __name__ == "__main__":
    main()
