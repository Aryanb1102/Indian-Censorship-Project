"""
Connectivity and content measurement pipeline for the censorship project.
Reads domains from data/domains.csv, performs DNS/TCP/HTTP/HTTPS/TLS checks, and
appends structured rows to data/raw_measurements.csv.
"""

import csv
import os
import socket
import ssl
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import dns.exception
import dns.resolver
import pandas as pd
import requests

DOMAINS_CSV_PATH = "data/domains.csv"
OUTPUT_CSV_PATH = "data/raw_measurements.csv"
DEFAULT_VANTAGE = "IN-home"

# Tunable constants
DNS_TIMEOUT = 4.0
TCP_TIMEOUT = 5.0
HTTP_TIMEOUT = 10.0
TLS_TIMEOUT = 6.0
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

# Enable TEST_MODE to only process the first TEST_LIMIT domains.
# Set to False for full runs across all included domains.
TEST_MODE = False
TEST_LIMIT = 3

# Phrases typical of Indian ISP/DoT block pages (lowercase matching).
BLOCKPAGE_PHRASES = [
    "blocked as per the instructions received from the department of telecommunications",
    "this url has been blocked as per order of the competent authority",
    "has been blocked as per the directions received from the competent authorities",
]

# Explicit field order for writing output CSVs (vantage-aware).
FIELDNAMES: List[str] = [
    "run_id",
    "vantage",
    "timestamp_utc",
    "domain",
    "category",
    "subcategory",
    "tcp_80_ok",
    "tcp_80_error",
    "tcp_443_ok",
    "tcp_443_error",
    "dns_local_ok",
    "dns_local_ips",
    "dns_local_error",
    "dns_public_ok",
    "dns_public_ips",
    "dns_public_error",
    "http_final_url",
    "http_status_code",
    "http_outcome",
    "http_error",
    "http_body_snippet",
    "tls_ok",
    "tls_issuer",
    "tls_not_before",
    "tls_not_after",
    "tls_error",
]


def utc_now_iso() -> str:
    """Return current UTC time in ISO 8601 format with Z suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def current_run_id() -> str:
    """Generate a run identifier like 20251211T123456Z."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_domains(csv_path: str, limit: Optional[int] = None) -> pd.DataFrame:
    """Load domain list and filter to include_flag == yes."""
    df = pd.read_csv(csv_path)
    df["include_flag"] = df["include_flag"].astype(str).str.strip().str.lower()
    df = df[df["include_flag"] == "yes"].copy()
    if limit is not None:
        df = df.head(limit)
    return df[["domain", "category", "subcategory"]].reset_index(drop=True)


def _query_dns(domain: str, nameservers: Optional[List[str]] = None) -> Dict[str, str]:
    """Resolve domain using optional nameservers, returning status, IPs, and error."""
    resolver = dns.resolver.Resolver()
    resolver.timeout = DNS_TIMEOUT
    resolver.lifetime = DNS_TIMEOUT
    if nameservers:
        resolver.nameservers = nameservers

    addrs: List[str] = []
    error = ""
    try:
        for record_type in ("A", "AAAA"):
            try:
                answers = resolver.resolve(domain, record_type)
                addrs.extend([ans.to_text() for ans in answers])
            except dns.resolver.NoAnswer:
                continue
        ok = len(addrs) > 0
        if not addrs:
            error = "No A/AAAA records"
    except dns.exception.DNSException as exc:
        ok = False
        error = f"{exc.__class__.__name__}: {exc}"
    except Exception as exc:  # pragma: no cover - defensive
        ok = False
        error = f"Unexpected: {exc}"
    return {
        "ok": ok,
        "ips": ";".join(addrs),
        "error": error,
    }


def resolve_dns(domain: str) -> Dict[str, object]:
    """Resolve DNS using local resolver and Google Public DNS."""
    local = _query_dns(domain)
    public = _query_dns(domain, ["8.8.8.8"])
    return {
        "dns_local_ok": local["ok"],
        "dns_local_ips": local["ips"],
        "dns_local_error": local["error"],
        "dns_public_ok": public["ok"],
        "dns_public_ips": public["ips"],
        "dns_public_error": public["error"],
    }


def check_tcp(domain: str, port: int) -> Tuple[bool, str]:
    """Attempt TCP connection to domain:port with timeout."""
    try:
        with socket.create_connection((domain, port), timeout=TCP_TIMEOUT):
            return True, ""
    except Exception as exc:  # pragma: no cover - network dependent
        return False, f"{exc.__class__.__name__}: {exc}"


def classify_http_outcome(
    status_code: Optional[int], errors: List[str], final_url: str
) -> str:
    """Map status/error info to a normalized outcome label."""
    if status_code is not None:
        if status_code in (401, 403):
            return "access_denied"
        if 200 <= status_code < 300:
            return "success"
        if 300 <= status_code < 400:
            return "redirect"
        if 400 <= status_code < 500:
            return "client_error"
        if 500 <= status_code < 600:
            return "server_error"
    if errors:
        error_text = " ".join(errors).lower()
        if "timeout" in error_text:
            return "timeout"
        if "connection" in error_text or "name or service not known" in error_text:
            return "connection_error"
        return "other_error"
    if final_url:
        return "success"
    return "no_response"


def _extract_body_snippet(resp: requests.Response, limit: int = 2000) -> str:
    """Safely extract a short text snippet from a response body."""
    try:
        content = resp.content[:limit]
        encoding = resp.encoding or resp.apparent_encoding or "utf-8"
        return content.decode(encoding, errors="replace")
    except Exception:
        return ""


def fetch_http_https(domain: str) -> Dict[str, object]:
    """
    Try HTTPS first, then HTTP if HTTPS raises a requests-level exception.
    Returns details including final URL, status, outcome label, errors, and body snippet.
    """
    session = requests.Session()
    headers = {"User-Agent": USER_AGENT}
    status_code: Optional[int] = None
    final_url = ""
    errors: List[str] = []
    body_snippet = ""

    def _request(url: str) -> requests.Response:
        return session.get(url, headers=headers, timeout=HTTP_TIMEOUT, allow_redirects=True)

    https_url = f"https://{domain}/"
    http_url = f"http://{domain}/"

    try:
        resp = _request(https_url)
        status_code = resp.status_code
        final_url = resp.url
        body_snippet = _extract_body_snippet(resp)
    except requests.exceptions.RequestException as exc:
        errors.append(f"HTTPS error: {exc}")
        try:
            resp = _request(http_url)
            status_code = resp.status_code
            final_url = resp.url
            body_snippet = _extract_body_snippet(resp)
        except requests.exceptions.RequestException as exc2:
            errors.append(f"HTTP error: {exc2}")

    outcome = classify_http_outcome(status_code, errors, final_url)
    # Blockpage detection based on snippet content.
    if body_snippet:
        snippet_lower = body_snippet.lower()
        if any(phrase in snippet_lower for phrase in BLOCKPAGE_PHRASES):
            outcome = "blockpage_india"

    combined_error = " | ".join(errors)
    return {
        "http_final_url": final_url,
        "http_status_code": status_code if status_code is not None else "",
        "http_outcome": outcome,
        "http_error": combined_error,
        "http_body_snippet": body_snippet,
    }


def _format_name(component: object) -> str:
    """
    Flatten issuer/subject sequences from ssl.getpeercert into a readable string.
    """
    if not component:
        return ""
    try:
        # component is typically a tuple of tuples: ((('commonName', 'Example'),),)
        parts = []
        for entry in component:
            if isinstance(entry, (list, tuple)):
                for k, v in entry:
                    parts.append(f"{k}={v}")
        return ", ".join(parts)
    except Exception:  # pragma: no cover - defensive
        return str(component)


def fetch_tls_cert(domain: str) -> Dict[str, object]:
    """Perform TLS handshake and extract certificate metadata."""
    context = ssl.create_default_context()
    try:
        with socket.create_connection((domain, 443), timeout=TLS_TIMEOUT) as sock:
            with context.wrap_socket(sock, server_hostname=domain) as ssock:
                cert = ssock.getpeercert()
    except Exception as exc:  # pragma: no cover - network dependent
        return {
            "tls_ok": False,
            "tls_issuer": "",
            "tls_not_before": "",
            "tls_not_after": "",
            "tls_error": f"{exc.__class__.__name__}: {exc}",
        }

    issuer = _format_name(cert.get("issuer"))
    not_before = cert.get("notBefore", "")
    not_after = cert.get("notAfter", "")
    return {
        "tls_ok": True,
        "tls_issuer": issuer,
        "tls_not_before": not_before,
        "tls_not_after": not_after,
        "tls_error": "",
    }


def measure_domain(
    domain: str, category: str, subcategory: str, run_id: str, vantage: str
) -> Dict[str, object]:
    """Measure a single domain across DNS/TCP/HTTP/TLS."""
    timestamp = utc_now_iso()
    dns_result = resolve_dns(domain)

    tcp_80_ok, tcp_80_err = check_tcp(domain, 80)
    tcp_443_ok, tcp_443_err = check_tcp(domain, 443)

    http_result = fetch_http_https(domain)
    tls_result = fetch_tls_cert(domain)

    row: Dict[str, object] = {
        "run_id": run_id,
        "vantage": vantage,
        "timestamp_utc": timestamp,
        "domain": domain,
        "category": category,
        "subcategory": subcategory,
        "tcp_80_ok": tcp_80_ok,
        "tcp_80_error": tcp_80_err,
        "tcp_443_ok": tcp_443_ok,
        "tcp_443_error": tcp_443_err,
    }
    row.update(dns_result)
    row.update(http_result)
    row.update(tls_result)
    return row


def migrate_output_columns(
    output_path: str,
    fieldnames: List[str],
    defaults: Optional[Dict[str, object]] = None,
) -> None:
    """
    If an existing output file lacks required columns, rewrite it with the
    expanded header and default values for legacy rows.
    """
    defaults = defaults or {}
    if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
        return

    with open(output_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        existing_fields = reader.fieldnames or []
        missing = [fn for fn in fieldnames if fn not in existing_fields]
        if not missing:
            return
        legacy_rows = list(reader)

    migrated_rows: List[Dict[str, object]] = []
    for row in legacy_rows:
        new_row = {fn: row.get(fn, defaults.get(fn, "")) for fn in fieldnames}
        migrated_rows.append(new_row)

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(migrated_rows)

    print(f"Migrated existing {output_path} to include missing columns: {missing}")


def write_rows(output_path: str, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    """Append rows to CSV, writing header if file is new or empty."""
    if not rows:
        return

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    file_exists = os.path.exists(output_path)
    needs_header = not file_exists or os.path.getsize(output_path) == 0

    with open(output_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if needs_header:
            writer.writeheader()
        writer.writerows(rows)


def main(vantage_override: Optional[str] = None) -> None:
    run_id = current_run_id()
    vantage = vantage_override if vantage_override is not None else (sys.argv[1] if len(sys.argv) > 1 else DEFAULT_VANTAGE)
    domain_limit = TEST_LIMIT if TEST_MODE else None
    domains_df = load_domains(DOMAINS_CSV_PATH, limit=domain_limit)
    total_domains = len(domains_df)
    print(
        f"Starting measurement run: run_id={run_id}, vantage={vantage}, domains={total_domains}"
    )
    migrate_output_columns(
        OUTPUT_CSV_PATH,
        FIELDNAMES,
        defaults={"vantage": "unknown", "http_body_snippet": ""},
    )
    measurements: List[Dict[str, object]] = []
    for idx, (_, row) in enumerate(domains_df.iterrows()):
        domain = str(row["domain"]).strip()
        category = str(row.get("category", "")).strip()
        subcategory = str(row.get("subcategory", "")).strip()
        try:
            measurements.append(
                measure_domain(domain, category, subcategory, run_id, vantage)
            )
            print(f"[{idx + 1}/{total_domains}] {domain} done")
        except Exception as exc:  # pragma: no cover - defensive catch-all
            measurements.append(
                {
                    "run_id": run_id,
                    "vantage": vantage,
                    "timestamp_utc": utc_now_iso(),
                    "domain": domain,
                    "category": category,
                    "subcategory": subcategory,
                    "dns_local_ok": False,
                    "dns_local_ips": "",
                    "dns_local_error": f"Pipeline error: {exc}",
                    "dns_public_ok": False,
                    "dns_public_ips": "",
                    "dns_public_error": f"Pipeline error: {exc}",
                    "tcp_80_ok": False,
                    "tcp_80_error": f"Pipeline error: {exc}",
                    "tcp_443_ok": False,
                    "tcp_443_error": f"Pipeline error: {exc}",
                    "http_final_url": "",
                    "http_status_code": "",
                    "http_outcome": "other_error",
                    "http_error": f"Pipeline error: {exc}",
                    "http_body_snippet": "",
                    "tls_ok": False,
                    "tls_issuer": "",
                    "tls_not_before": "",
                    "tls_not_after": "",
                    "tls_error": f"Pipeline error: {exc}",
                }
            )
            print(f"[{idx + 1}/{total_domains}] {domain} done (with error)")
    write_rows(OUTPUT_CSV_PATH, measurements, FIELDNAMES)
    print(
        f"Wrote {len(measurements)} rows to {OUTPUT_CSV_PATH} "
        f"(run_id={run_id}, vantage={vantage})."
    )
    if TEST_MODE:
        has_body = any(
            (row.get("http_body_snippet") or "") and row.get("http_outcome") == "success"
            for row in measurements
        )
        msg = (
            "Blockpage detection: http_body_snippet present and http_outcome classification OK"
            if has_body
            else "Blockpage detection: missing body snippets for successful domains"
        )
        print(msg)
    print(f"Finished measurement run: run_id={run_id}, vantage={vantage}")
    if TEST_MODE:
        print("TEST_MODE is enabled; only first TEST_LIMIT domains were processed.")


if __name__ == "__main__":
    main()
