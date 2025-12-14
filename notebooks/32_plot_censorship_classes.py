"""
Plot counts of unique domains per censorship_class.

Run: python notebooks/32_plot_censorship_classes.py
"""

import os

import pandas as pd
import matplotlib.pyplot as plt

INPUT_PATH = "data/domain_level_summary_enriched.csv"
FIG_PATH = "figures/censorship_class_counts.png"


def main() -> None:
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Missing input file: {INPUT_PATH}")
    df = pd.read_csv(INPUT_PATH)
    if df.empty:
        raise ValueError(f"No rows in {INPUT_PATH}")
    if "censorship_class" not in df.columns:
        raise ValueError("Expected 'censorship_class' column in input data.")

    counts = df["censorship_class"].value_counts()
    print("Domains per censorship_class:")
    print(counts)

    counts_sorted = counts.sort_values(ascending=False)
    plt.figure(figsize=(10, 6))
    ax = counts_sorted.plot(kind="bar")
    ax.set_ylabel("Number of domains")
    ax.set_xlabel("censorship_class")
    ax.set_title("Domains per censorship class")
    plt.xticks(rotation=25)
    plt.tight_layout()

    os.makedirs(os.path.dirname(FIG_PATH), exist_ok=True)
    plt.savefig(FIG_PATH, dpi=200)
    plt.close()
    print(f"Saved figure to {FIG_PATH}")


if __name__ == "__main__":
    main()
