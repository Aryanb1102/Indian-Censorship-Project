import csv
from collections import Counter

PATH = "data/raw_measurements.csv"

def main():
    print("Reading:", PATH)
    with open(PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print("Total data rows:", len(rows))
    if not rows:
        return

    print("Columns:", list(rows[0].keys()))

    run_id_counts = Counter(r["run_id"] for r in rows)
    print("run_id counts:", dict(run_id_counts))

    # Show first 3 rows as a sample
    print("\nSample rows:")
    for r in rows[:3]:
        print(
            r["domain"],
            "| category:", r["category"],
            "| http_status_code:", r["http_status_code"],
            "| http_outcome:", r["http_outcome"],
            "| tls_issuer:", (r["tls_issuer"] or "")[:50],
        )

if __name__ == "__main__":
    main()
