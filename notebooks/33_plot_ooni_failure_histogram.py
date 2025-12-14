"""
Plot histogram of OONI failure rates across domains.

Run: python notebooks/33_plot_ooni_failure_histogram.py
"""

import os

import pandas as pd
import matplotlib.pyplot as plt

INPUT_PATH = "data/domain_level_summary_enriched.csv"
FIG_PATH = "figures/ooni_failure_rate_hist.png"


def main() -> None:
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Missing input file: {INPUT_PATH}")
    df = pd.read_csv(INPUT_PATH)
    required_cols = {"ooni_failure_rate", "ooni_total_measurements", "domain", "category"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df[df["ooni_total_measurements"] > 0].copy()
    if df.empty:
        raise ValueError("No rows with OONI measurements > 0.")

    bins = [i / 10 for i in range(0, 11)]  # 0.0 to 1.0 in steps of 0.1
    labels = [f"{bins[i]:.1f}–{bins[i+1]:.1f}" for i in range(len(bins) - 1)]
    df["failure_bin"] = pd.cut(df["ooni_failure_rate"], bins=bins, labels=labels, include_lowest=True, right=True)

    counts = df["failure_bin"].value_counts().reindex(labels, fill_value=0)

    print("Top 10 domains by OONI failure rate:")
    top10 = df.sort_values("ooni_failure_rate", ascending=False).head(10)
    cols_to_show = ["domain", "category", "ooni_failure_rate", "censorship_class"] if "censorship_class" in df.columns else ["domain", "category", "ooni_failure_rate"]
    print(top10[cols_to_show])

    plt.figure(figsize=(10, 6))
    ax = counts.plot(kind="bar")
    ax.set_xlabel("OONI failure rate")
    ax.set_ylabel("Number of domains")
    ax.set_title("OONI failure rate across domains (OONI Web Connectivity)")
    plt.xticks(rotation=25)
    plt.tight_layout()

    os.makedirs(os.path.dirname(FIG_PATH), exist_ok=True)
    plt.savefig(FIG_PATH, dpi=200)
    plt.close()
    print(f"Saved figure to {FIG_PATH}")


if __name__ == "__main__":
    main()
