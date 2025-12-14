"""
Compare two vantages (latest runs) at domain level.

Usage:
python notebooks/25_compare_vantages.py IN-home VPN-EU
"""

import argparse
from pathlib import Path
import sys
import pandas as pd

RAW_PATH = Path("data/raw_measurements.csv")
OUTPUT_DIR = Path("data")


def most_common(series: pd.Series):
    """Return most frequent non-null value, else empty string."""
    vc = series.dropna().value_counts()
    return vc.idxmax() if not vc.empty else ""


def load_latest_for_vantage(df: pd.DataFrame, vantage: str) -> pd.DataFrame:
    """Filter to latest run for a vantage; raise if missing."""
    df_v = df[df["vantage"] == vantage]
    if df_v.empty:
        raise ValueError(f"No rows found for vantage '{vantage}'")
    latest_run = df_v["run_id"].max()
    df_run = df_v[df_v["run_id"] == latest_run].copy()
    if df_run.empty:
        raise ValueError(f"No rows for vantage '{vantage}' at run_id {latest_run}")
    return df_run


def aggregate_vantage(df_run: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Aggregate per domain for a given vantage run."""
    grouped = (
        df_run.groupby(["domain", "category", "subcategory"])
        .agg(
            http_outcome=("http_outcome", most_common),
            http_status_code=("http_status_code", most_common),
            blockpage_flag=("http_outcome", lambda s: (s == "blockpage_india").any()),
        )
        .reset_index()
    )
    grouped = grouped.rename(
        columns={
            "http_outcome": f"{prefix}_http_outcome",
            "http_status_code": f"{prefix}_status_code",
            "blockpage_flag": f"{prefix}_blockpage_flag",
        }
    )
    return grouped


def is_success(outcome: str) -> bool:
    """Treat success/redirect as success."""
    return str(outcome).lower() in {"success", "redirect"}


def classify_row(row: pd.Series) -> str:
    """Classify differences between local and remote vantage outcomes."""
    local_outcome = str(row.get("local_http_outcome", "")).lower()
    remote_outcome = str(row.get("remote_http_outcome", "")).lower()
    local_block = bool(row.get("local_blockpage_flag", False))
    remote_block = bool(row.get("remote_blockpage_flag", False))

    local_success = is_success(local_outcome)
    remote_success = is_success(remote_outcome)

    if local_success and remote_success:
        return "both_success"
    if local_block and remote_success:
        return "india_blockpage_remote_ok"
    if (not local_success) and remote_success and not local_block:
        return "india_blocked_remote_ok"
    if local_success and (not remote_success):
        return "remote_blocked_india_ok"
    if (not local_success) and (not remote_success):
        return "both_bad"
    if remote_block and local_success:
        return "remote_blockpage_india_ok"
    return "other"


def compare_vantages(local_vantage: str, remote_vantage: str) -> pd.DataFrame:
    """Load raw data and produce comparison DataFrame."""
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"Missing file: {RAW_PATH}")
    df = pd.read_csv(RAW_PATH)
    if "vantage" not in df.columns:
        raise ValueError("Expected 'vantage' column in raw_measurements.csv")

    local_df = load_latest_for_vantage(df, local_vantage)
    remote_df = load_latest_for_vantage(df, remote_vantage)

    local_agg = aggregate_vantage(local_df, "local")
    remote_agg = aggregate_vantage(remote_df, "remote")

    merged = local_agg.merge(remote_agg, on="domain", how="left", suffixes=("", "_remote"))
    merged["vantage_diff_flag"] = merged.apply(classify_row, axis=1)
    return merged


def print_summary(df: pd.DataFrame) -> None:
    """Print summary stats for comparison."""
    print("Counts by vantage_diff_flag:")
    print(df["vantage_diff_flag"].value_counts())

    suspicious_flags = {"india_blocked_remote_ok", "india_blockpage_remote_ok"}
    suspicious = df[df["vantage_diff_flag"].isin(suspicious_flags)]
    if not suspicious.empty:
        print("\nTop 10 suspicious domains (India problematic, remote ok):")
        cols = [
            "domain",
            "category",
            "local_http_outcome",
            "local_status_code",
            "remote_http_outcome",
            "remote_status_code",
            "vantage_diff_flag",
        ]
        print(suspicious[cols].head(10).to_string(index=False))
    else:
        print("\nNo domains flagged as suspicious in this comparison.")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Compare latest runs across two vantages.")
    parser.add_argument("local_vantage", help="Local/India vantage label (e.g., IN-home)")
    parser.add_argument("remote_vantage", help="Remote/VPN vantage label (e.g., VPN-EU)")
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    try:
        df_comp = compare_vantages(args.local_vantage, args.remote_vantage)
    except ValueError as exc:
        print(f"Error: {exc}")
        sys.exit(1)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"vantage_comparison_{args.local_vantage}_vs_{args.remote_vantage}.csv"
    df_comp.to_csv(out_path, index=False)
    print(f"Saved comparison to {out_path}")
    print_summary(df_comp)


if __name__ == "__main__":
    main()
