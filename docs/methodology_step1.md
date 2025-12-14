# Technical Measurement Plan – Step 1

## Objective

- Build a curated list of approximately 100 India-relevant domains across four categories:  
  **News & Politics**, **Social & Streaming**, **Government & Public Services**, and **Civil Society & NGOs**.
- For each domain, perform multi-layer network measurements from an India-based vantage point, covering:  
  **DNS resolution**, **TCP connectivity**, **HTTP/HTTPS behaviour**, and **TLS certificate properties**.
- Log results in a structured, machine-readable format so that domain behaviour can be compared:
  - Across categories (e.g., mainstream vs independent media, government vs civil society).
  - Across technical layers (e.g., DNS failures vs HTTP block pages).
- Lay the technical groundwork for later analysis of censorship, geoblocking, and infrastructure differences in the Indian internet context, without yet making strong normative or political claims.

---

## Per-domain tests

For each domain in `data/domains.csv`, the measurement pipeline will execute the following tests from the local network context (India):

### 1. DNS Resolution

- Perform DNS resolution using:
  - The **system/local resolver** (whatever DNS the ISP or OS is configured to use).
  - A **public resolver** (e.g., `8.8.8.8` – Google Public DNS) as a control.
- For each resolver, log:
  - The full set of returned IP addresses (IPv4 and IPv6 where applicable).
  - Any errors such as `NXDOMAIN`, `SERVFAIL`, timeouts, or unexpected responses.
- Compare local vs public resolver outputs to flag potential anomalies:
  - Domain resolves locally but not on public resolver (or vice versa).
  - Different IP sets between local and public, especially if local points to private or unusual ranges.

### 2. TCP Connectivity

- Attempt to establish TCP connections to:
  - Port **80** (HTTP), where applicable.
  - Port **443** (HTTPS), which should be present for most modern sites.
- For each (domain, port) pair, log:
  - Success or failure of the TCP handshake.
  - Any immediate connection errors (e.g., connection refused, timeout, network unreachable, reset).
- Use this to distinguish:
  - Basic reachability problems (e.g., host down, routing issues).
  - Potential port-specific blocking (e.g., HTTP blocked but HTTPS allowed, or vice versa).

### 3. HTTP / HTTPS Behaviour

- For HTTP(S), perform HTTP requests using a standard user-agent and conservative timeouts:
  - For HTTPS: `GET https://<domain>/`
  - Optionally, for fallback: `GET http://<domain>/` when HTTPS fails or is not supported.
- For each request, log:
  - HTTP **status code** (e.g., 200, 301, 302, 403, 451, 503, etc.).
  - Final URL after redirects (to capture canonical hostnames, geo-redirects, or block pages).
  - High-level outcome: success, redirect, client error, server error, timeout.
  - Any clearly recognisable block-page content patterns (e.g., phrases like “blocked in your country”, ISP splash pages, legal notices), as simple text flags rather than full-body storage.
- This layer is used to:
  - Identify cases where DNS and TCP succeed, but HTTP returns errors or specialised block pages.
  - See how different categories of sites respond to requests from Indian IP space (e.g., normal content vs access-denied pages).

### 4. TLS / Certificate Layer

- For domains reachable over HTTPS, initiate a TLS handshake and extract certificate metadata.
- For each domain, log:
  - **Certificate issuer** (Certificate Authority – CA).
  - **Subject** / common name (CN) and subject alternative names (SANs), if easily available.
  - **Validity period** (start and end dates).
  - Whether the certificate appears valid from the client’s perspective (e.g., not expired, not self-signed/untrusted by the default trust store).
- Use this information to:
  - Examine which CAs dominate for different categories (e.g., government vs mainstream media vs independent outlets).
  - Flag unusual or self-signed certificates, especially on government or high-profile domains, which may hint at non-standard infrastructure or interception.

---

## Expected Outputs

- A **raw measurement CSV** with at least one row per domain (and ideally one row per domain per measurement run), containing:
  - Domain name and its category/subcategory metadata.
  - DNS results (local vs public resolver IPs and errors).
  - TCP connectivity outcomes for ports 80 and 443.
  - HTTP/HTTPS status codes, final URLs, and high-level outcome labels.
  - TLS certificate issuer, subject, validity window, and basic trust/validity flag.
- A clear labelling scheme for potential anomalies at each layer, including:
  - DNS-level issues (e.g., `dns_nxdomain`, `dns_timeout`, `dns_mismatch_local_vs_public`).
  - TCP-level failures (e.g., `tcp_timeout`, `tcp_reset`, `tcp_refused`).
  - HTTP-level issues (e.g., `http_403`, `http_451`, `http_timeout`, `http_blockpage_suspected`).
- Category-level summaries derived from the raw CSV, such as:
  - Distribution of HTTP status codes by category (e.g., government vs independent media).
  - Frequency and types of DNS/TCP failures across different categories.
  - Distribution of certificate issuers across government, mainstream media, independent media, and NGOs.
- A foundation for later **geoblocking analysis** (optional extension in future steps), where the same measurement schema could be reused from a non-Indian vantage point (e.g., VPN exit in another country) and compared against the India-based results.
- Overall, a reproducible, transparent dataset that documents how this curated set of India-relevant websites behaves from the perspective of an Indian user, across DNS, TCP, HTTP/HTTPS, and TLS layers.
