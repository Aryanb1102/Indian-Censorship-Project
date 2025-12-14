"""
Plot domain counts by category.

Run: python notebooks/31_plot_domain_counts_by_category.py
"""

import os

import pandas as pd
import matplotlib.pyplot as plt

INPUT_PATH = "data/domain_level_summary_enriched.csv"
FIG_PATH = "figures/domain_counts_by_category.png"


def main() -> None:
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Missing input file: {INPUT_PATH}")
    df = pd.read_csv(INPUT_PATH)
    if df.empty:
        raise ValueError(f"No rows in {INPUT_PATH}")
    if "category" not in df.columns:
        raise ValueError("Expected 'category' column in input data.")

    counts = df["category"].value_counts()
    print("Domain counts by category:")
    print(counts)

    plt.figure(figsize=(10, 6))
    ax = counts.sort_index().plot(kind="bar")
    ax.set_ylabel("Number of domains")
    ax.set_xlabel("Category")
    ax.set_title("Domain counts by category")
    plt.xticks(rotation=25)
    plt.tight_layout()

    os.makedirs(os.path.dirname(FIG_PATH), exist_ok=True)
    plt.savefig(FIG_PATH, dpi=200)
    plt.close()
    print(f"Saved figure to {FIG_PATH}")


if __name__ == "__main__":
    main()
