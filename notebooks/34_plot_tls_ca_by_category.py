"""
Plot TLS CA ecosystem by category for latest IN-home run.

Run: python notebooks/34_plot_tls_ca_by_category.py
"""

import os

import pandas as pd
import matplotlib.pyplot as plt

RAW_PATH = "data/raw_measurements.csv"
FIG_PATH = "figures/tls_ca_by_category.png"
VANTAGE = "IN-home"


def map_ca_group(issuer: str) -> str:
    """Map issuer string to coarse CA group."""
    if not issuer or str(issuer).strip().lower() in {"", "no_cert"}:
        return "Gov/Other"
    text = str(issuer)
    lower = text.lower()
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


def main() -> None:
    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError(f"Missing input file: {RAW_PATH}")
    df = pd.read_csv(RAW_PATH)
    required_cols = {"run_id", "vantage", "tls_ok", "tls_issuer", "category", "domain"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df_v = df[df["vantage"] == VANTAGE]
    if df_v.empty:
        raise ValueError(f"No rows for vantage '{VANTAGE}' in {RAW_PATH}")
    latest_run = df_v["run_id"].max()
    df_run = df_v[df_v["run_id"] == latest_run].copy()
    if df_run.empty:
        raise ValueError(f"No rows for vantage '{VANTAGE}' at run_id {latest_run}")

    df_run = df_run[df_run["tls_ok"] == True]  # noqa: E712
    df_run = df_run[df_run["category"].notna()]
    if df_run.empty:
        raise ValueError("No rows with tls_ok == True and non-null category.")

    df_run["ca_group"] = df_run["tls_issuer"].fillna("").apply(map_ca_group)

    # Count unique domains per (category, ca_group)
    grouped = (
        df_run.groupby(["category", "ca_group"])["domain"]
        .nunique()
        .reset_index(name="domain_count")
    )
    pivot = grouped.pivot(index="category", columns="ca_group", values="domain_count").fillna(0).astype(int)

    print("Category x CA group (unique domains):")
    print(pivot)

    plt.figure(figsize=(12, 7))
    pivot.sort_index().plot(kind="bar", stacked=True, ax=plt.gca())
    plt.ylabel("Number of domains")
    plt.xlabel("Category")
    plt.title("TLS CA ecosystem by category (IN-home vantage)")
    plt.xticks(rotation=25)
    plt.tight_layout()

    os.makedirs(os.path.dirname(FIG_PATH), exist_ok=True)
    plt.savefig(FIG_PATH, dpi=200)
    plt.close()
    print(f"Saved figure to {FIG_PATH}")


if __name__ == "__main__":
    main()
