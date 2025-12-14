"""
Assign censorship_class to domains using local outcomes, OONI rates, and optional
vantage comparisons.

Run: python notebooks/30_classify_domains.py [--vantage-comparison-file PATH]
"""

import argparse
from pathlib import Path
import pandas as pd

SUMMARY_PATH = Path("data/domain_level_summary.csv")
RAW_PATH = Path("data/raw_measurements.csv")
OUTPUT_PATH = Path("data/domain_level_summary_enriched.csv")


def load_summary() -> pd.DataFrame:
    if not SUMMARY_PATH.exists():
        raise FileNotFoundError(f"Missing summary file: {SUMMARY_PATH}")
    df = pd.read_csv(SUMMARY_PATH)
    if df.empty:
        raise ValueError(f"No rows in {SUMMARY_PATH}")
    for col in ["ooni_total_measurements", "ooni_failure_count", "ooni_failure_rate"]:
        if col in df.columns:
            df[col] = df[col].fillna(0)
    return df


def find_vantage_file(local_vantage: str) -> Path | None:
    pattern = f"data/vantage_comparison_{local_vantage}_vs_"
    for p in Path("data").glob(f"vantage_comparison_{local_vantage}_vs_*.csv"):
        return p
    return None


def load_vantage_diff(path: Path | None) -> pd.DataFrame | None:
    if path is None:
        return None
    if not path.exists():
        return None
    df = pd.read_csv(path)
    if df.empty:
        return None
    return df[["domain", "vantage_diff_flag"]]


def load_blockpage_flags(local_vantage: str = "IN-home") -> pd.Series:
    """Return a Series indexed by domain indicating blockpage presence in raw data."""
    if not RAW_PATH.exists():
        return pd.Series(dtype=bool)
    df = pd.read_csv(RAW_PATH)
    if df.empty or "http_outcome" not in df.columns:
        return pd.Series(dtype=bool)
    mask = df["http_outcome"].str.lower() == "blockpage_india"
    if "vantage" in df.columns:
        mask = mask & (df["vantage"] == local_vantage)
    block_domains = df.loc[mask, "domain"].dropna().unique()
    return pd.Series(True, index=block_domains)


def classify_row(row: pd.Series, blockpage_flags: pd.Series) -> str:
    """Assign censorship_class based on heuristics."""
    category = str(row.get("category", "") or "")
    subcategory = str(row.get("subcategory", "") or "")
    domain = str(row.get("domain", "") or "")
    local_outcome = str(row.get("local_http_outcome", "") or "").lower()
    ooni_rate = float(row.get("ooni_failure_rate", 0) or 0)
    vantage_flag = str(row.get("vantage_diff_flag", "unknown") or "unknown")
    has_blockpage = blockpage_flags.get(domain, False)

    # Helpers
    def is_success(outcome: str) -> bool:
        return outcome in {"success", "redirect"}

    # policy_blocked: social/streaming + likely adult/torrent + high failure or blockpage
    if category == "Social & Streaming" and (
        "adult" in subcategory.lower() or "porn" in subcategory.lower() or "torrent" in subcategory.lower()
    ):
        if (
            ooni_rate >= 0.9
            or local_outcome == "blockpage_india"
            or has_blockpage
            or vantage_flag == "india_blockpage_remote_ok"
        ):
            return "policy_blocked"

    # app_ban_partial: banned apps with moderate failure
    banned_apps = {"tiktok.com", "telegram.org", "snapchat.com", "xvideos.com"}
    if domain in banned_apps and 0.2 <= ooni_rate < 0.9:
        return "app_ban_partial"

    # rights_org_suspect: civil society targets with any signal of trouble
    if category == "Civil Society & NGOs":
        if ooni_rate > 0 or vantage_flag in {"india_blocked_remote_ok", "india_blockpage_remote_ok"} or has_blockpage:
            return "rights_org_suspect"

    # infra_flaky: gov sites with local connectivity errors but clean OONI
    if category.startswith("Government"):
        if local_outcome in {"timeout", "connection_error"} and ooni_rate == 0:
            return "infra_flaky"

    # Normal case: successes or benign outcomes
    return "normal"


def enrich(local_vantage: str, comp_path: Path | None) -> pd.DataFrame:
    summary = load_summary()
    block_flags = load_blockpage_flags(local_vantage)
    comp_df = load_vantage_diff(comp_path)
    if comp_df is None:
        summary["vantage_diff_flag"] = "unknown"
    else:
        summary = summary.merge(comp_df, on="domain", how="left")
        summary["vantage_diff_flag"] = summary["vantage_diff_flag"].fillna("unknown")

    summary["censorship_class"] = summary.apply(
        lambda r: classify_row(r, block_flags), axis=1
    )
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUTPUT_PATH, index=False)
    return summary


def print_outputs(df: pd.DataFrame) -> None:
    print("censorship_class counts:")
    print(df["censorship_class"].value_counts())
    print("\nSample domains per class:")
    for cls in df["censorship_class"].unique():
        sample = df[df["censorship_class"] == cls].head(5)
        print(f"\nClass: {cls}")
        print(sample[["domain", "category", "subcategory", "censorship_class"]].to_string(index=False))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Enrich domain summary with censorship classes.")
    parser.add_argument(
        "--vantage-comparison-file",
        dest="comp_file",
        help="Optional path to a vantage comparison CSV. If omitted, auto-detects IN-home vs VPN-*.",
    )
    parser.add_argument(
        "--local-vantage",
        dest="local_vantage",
        default="IN-home",
        help="Local vantage label to use for blockpage flags (default IN-home).",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    comp_path = Path(args.comp_file) if args.comp_file else find_vantage_file(args.local_vantage)
    df = enrich(args.local_vantage, comp_path)
    print_outputs(df)


if __name__ == "__main__":
    main()
