"""
Visualize HTTP outcome differences between IN-home and VPN-UK vantages.

Run: python notebooks/26_plot_vantage_http_diff.py
"""

from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

INPUT_PATH = Path("data/vantage_comparison_IN-home_vs_VPN-UK.csv")
FIG_PATH = Path("figures/vantage_http_diff_matrix_IN-home_vs_VPN-UK.png")


def simplify_outcome(x: object) -> str:
    """Reduce outcome to success/fail."""
    if str(x).lower() == "success":
        return "success"
    return "fail"


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_PATH}")
    df = pd.read_csv(INPUT_PATH)
    required_cols = {"local_http_outcome", "remote_http_outcome"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["local_simple"] = df["local_http_outcome"].apply(simplify_outcome)
    df["remote_simple"] = df["remote_http_outcome"].apply(simplify_outcome)

    ctab = pd.crosstab(df["local_simple"], df["remote_simple"])
    print("HTTP outcome crosstab (local=IN-home, remote=VPN-UK):")
    print(ctab)

    # Flatten combinations for a simple bar chart
    combinations = [
        ("IN-home=success / VPN-UK=success", "success", "success"),
        ("IN-home=fail / VPN-UK=success", "fail", "success"),
        ("IN-home=success / VPN-UK=fail", "success", "fail"),
        ("IN-home=fail / VPN-UK=fail", "fail", "fail"),
    ]

    labels = []
    values = []
    for label, lval, rval in combinations:
        labels.append(label)
        values.append(ctab.get(rval, pd.Series()).get(lval, 0))

    plt.figure(figsize=(10, 6))
    ax = plt.bar(labels, values)
    plt.ylabel("Number of domains")
    plt.xlabel("Outcome combination")
    plt.title("HTTP outcome differences: IN-home vs VPN-UK")
    plt.xticks(rotation=20)
    plt.tight_layout()

    FIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(FIG_PATH, dpi=200)
    plt.close()
    print(f"Saved figure to {FIG_PATH}")


if __name__ == "__main__":
    main()
