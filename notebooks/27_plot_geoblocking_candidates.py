"""
Identify and plot geoblocking-like patterns by category comparing IN-home vs VPN-UK.

Run: python notebooks/27_plot_geoblocking_candidates.py
"""

from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

INPUT_PATH = Path("data/vantage_comparison_IN-home_vs_VPN-UK.csv")
FIG_PATH = Path("figures/geoblocking_candidates_by_category_IN-home_vs_VPN-UK.png")


def simplify_outcome(x: object) -> str:
    """Reduce outcome to success/fail."""
    if str(x).lower() == "success":
        return "success"
    return "fail"


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_PATH}")
    df = pd.read_csv(INPUT_PATH)
    required_cols = {"local_http_outcome", "remote_http_outcome", "category", "domain"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["local_simple"] = df["local_http_outcome"].apply(simplify_outcome)
    df["remote_simple"] = df["remote_http_outcome"].apply(simplify_outcome)
    df["geoblocking_candidate"] = (df["local_simple"] == "fail") & (df["remote_simple"] == "success")

    candidates = df[df["geoblocking_candidate"]]
    if not candidates.empty:
        print("Geoblocking candidates (IN-home fail, VPN-UK success):")
        print(
            candidates[
                ["domain", "category", "local_http_outcome", "remote_http_outcome"]
            ].to_string(index=False)
        )
    else:
        print("No geoblocking candidates found.")

    grp = (
        df.groupby(["category", "geoblocking_candidate"])["domain"]
        .nunique()
        .reset_index(name="count")
    )
    pivot = grp.pivot(index="category", columns="geoblocking_candidate", values="count").fillna(0)
    # Ensure both columns exist and reorder
    for col in [True, False]:
        if col not in pivot.columns:
            pivot[col] = 0
    pivot = pivot.reindex(columns=[True, False]).rename(
        columns={True: "geoblocking_candidate", False: "not_geoblocked"}
    )

    print("\nCategory breakdown (unique domains):")
    print(pivot)

    pivot.sort_index().plot(
        kind="bar",
        stacked=True,
        figsize=(12, 7),
        title="Geoblocking candidates by category (IN-home vs VPN-UK)",
    )
    plt.xlabel("Category")
    plt.ylabel("Number of domains")
    plt.xticks(rotation=20)
    plt.legend(title="", labels=["Geoblocking candidate", "Other"])
    plt.tight_layout()

    FIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(FIG_PATH, dpi=200)
    plt.close()
    print(f"\nSaved figure to {FIG_PATH}")


if __name__ == "__main__":
    main()
