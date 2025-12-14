"""
Lite OONI ingestion: fetch web_connectivity measurements for India via API,
filter to project domains, and save a cleaned CSV.
"""

import time
import csv
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse

import pandas as pd
import requests

BASE_URL = "https://api.ooni.io/api/v1/measurements"
PROBE_CC = "IN"
TEST_NAME = "web_connectivity"
SINCE = "2024-01-01"
MAX_MEASUREMENTS = 8000
PAGE_SLEEP_SECONDS = 0.3
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3

OUTPUT_PATH = "data/ooni_india_webconnectivity_clean.csv"
DOMAINS_PATH = "data/domains.csv"


def fetch_measurements() -> List[Dict]:
    """Fetch paginated OONI measurements with basic retry logic."""
    measurements: List[Dict] = []
    url: Optional[str] = BASE_URL
    params = {"probe_cc": PROBE_CC, "test_name": TEST_NAME, "since": SINCE}

    while url and len(measurements) < MAX_MEASUREMENTS:
        last_error: Optional[Exception] = None
        response = None
        for _ in range(MAX_RETRIES):
            try:
                response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                break
            except Exception as exc:
                last_error = exc
                time.sleep(1)
        if response is None:
            raise RuntimeError(f"Failed to fetch {url}: {last_error}")

        data = response.json()
        batch = data.get("results", []) or []
        measurements.extend(batch)
        if len(measurements) >= MAX_MEASUREMENTS:
            break

        metadata = data.get("metadata", {}) or {}
        next_url = data.get("next_url") or metadata.get("next_url")
        url = next_url
        params = None  # next_url already contains query params if present
        if url:
            time.sleep(PAGE_SLEEP_SECONDS)
    return measurements[:MAX_MEASUREMENTS]


def normalize_domain(input_url: str) -> Tuple[str, str]:
    """
    Normalize input to a bare domain for matching:
    returns (bare_domain_without_www, original_hostname_lower).
    """
    if not input_url:
        return "", ""
    parsed = urlparse(input_url if "://" in input_url else f"//{input_url}", scheme="http")
    hostname = (parsed.hostname or "").lower()
    bare = hostname[4:] if hostname.startswith("www.") else hostname
    return bare, hostname


def load_allowed_domains(path: str) -> set:
    """Load domains.csv and return canonical set (stripped of leading www)."""
    df = pd.read_csv(path)
    df["include_flag"] = df["include_flag"].astype(str).str.lower()
    df = df[df["include_flag"] == "yes"]
    domains = df["domain"].astype(str).str.lower().str.strip()
    return {d[4:] if d.startswith("www.") else d for d in domains}


def filter_and_clean(measurements: List[Dict], allowed_domains: set) -> List[Dict[str, object]]:
    """Filter to allowed domains and keep a cleaned subset of fields."""
    cleaned: List[Dict[str, object]] = []
    for m in measurements:
        input_url = m.get("input", "")
        domain_norm, hostname = normalize_domain(input_url)
        if not domain_norm or domain_norm not in allowed_domains:
            continue
        analysis = m.get("analysis", {}) or {}
        cleaned.append(
            {
                "measurement_start_time": m.get("measurement_start_time", ""),
                "domain": domain_norm,
                "input": input_url,
                "probe_cc": m.get("probe_cc", ""),
                "probe_asn": m.get("probe_asn", ""),
                "test_name": m.get("test_name", ""),
                "failure": m.get("failure", ""),
                "anomaly": m.get("anomaly", ""),
                "analysis_anomaly": analysis.get("anomaly", ""),
                "blocking_general": analysis.get("blocking_general", ""),
                "blocking_type": analysis.get("blocking_type", ""),
            }
        )
    return cleaned


def save_cleaned(rows: List[Dict[str, object]], path: str) -> None:
    """Write cleaned rows to CSV."""
    if not rows:
        raise ValueError("No filtered OONI measurements to save.")
    fieldnames = list(rows[0].keys())
    with open(path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    allowed_domains = load_allowed_domains(DOMAINS_PATH)
    print(f"Loaded {len(allowed_domains)} allowed domains from {DOMAINS_PATH}")

    measurements = fetch_measurements()
    print(f"Fetched {len(measurements)} OONI measurements (capped at {MAX_MEASUREMENTS})")

    cleaned = filter_and_clean(measurements, allowed_domains)
    print(f"Filtered to {len(cleaned)} measurements matching project domains")

    # Breakdown by domain
    if cleaned:
        df_clean = pd.DataFrame(cleaned)
        counts = df_clean["domain"].value_counts()
        print("\nTop matched domains (count):")
        print(counts.head(20))

    save_cleaned(cleaned, OUTPUT_PATH)
    print(f"Saved cleaned OONI data to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
