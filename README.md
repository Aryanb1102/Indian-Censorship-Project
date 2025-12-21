## India Censorship Measurement Project

This repo measures connectivity from India to ~100 domains (news, platforms, government, NGOs), combines local measurements with OONI web connectivity data, compares vantages (home vs VPN), and classifies domains into censorship-related categories. Everything is Python and runs locally; a Streamlit dashboard is included.

### Setup (Windows-friendly)
1) Create/activate venv (from repo root):
```
python -m venv venv
.\venv\Scripts\Activate.ps1
```
2) Install dependencies:
```
pip install -r requirements.txt
```

### Running the pipeline (main.py)
Main orchestrator lives at `main.py`. Flags call the underlying scripts directly (no shelling out).

- Full local run (single vantage, e.g. home):
```
python main.py --do-measure --vantage IN-home --do-ooni --do-merge --do-analyze --do-classify --print-summary
```
- After collecting a VPN vantage (e.g. VPN-UK), run comparison + classify + summary:
```
python main.py --do-vantage-compare --vantage IN-home --remote-vantage VPN-UK --do-classify --print-summary
```
- You can mix flags as needed; `--vantage` controls measurement/analysis, `--remote-vantage` controls comparison. `--comp-file` can point `--do-classify` to a specific comparison CSV.

### Key outputs
- Raw measurements: `data/raw_measurements.csv` (one row per domain per run, with `vantage`, `http_body_snippet`, DNS/TCP/HTTP/TLS fields).
- OONI ingest: `data/ooni_india_webconnectivity_clean.csv`.
- Local+OONI merge: `data/domain_level_summary.csv`.
- Vantage comparison: `data/vantage_comparison_<local>_vs_<remote>.csv` (e.g., `IN-home_vs_VPN-UK`).
- Enriched classification: `data/domain_level_summary_enriched.csv`.
- Figures and summaries:
  - `figures/http_outcomes_by_category.png`
  - `data/http_outcomes_by_category.csv`
  - `data/tls_issuers_*.csv`
  - `data/dns_mismatch_local_vs_public.csv`
  - Vantage/geoblocking/tls/ooni plots: `figures/vantage_http_diff_matrix_*.png`, `figures/geoblocking_candidates_by_category_*.png`, `figures/tls_ca_by_category.png`, `figures/ooni_failure_rate_hist.png`, `figures/domain_counts_by_category.png`, `figures/censorship_class_counts.png`

### Dashboard (Streamlit)
Launch the interactive dashboard (with vantage/geoblocking/TLS CA/class views):
```
streamlit run app/dashboard.py --server.headless true --server.port 8501 --server.address 127.0.0.1 --browser.gatherUsageStats false
```
Features:
- Filters: category, vantage, latest/all runs, run_id selection, vantage comparison file selection.
- Tabs:
  - Overview: key metrics, HTTP outcomes, TLS issuers, problematic domains.
  - TLS CA: stacked CA groups by category.
  - Vantage diff: outcome crosstab and bar view (IN-home vs VPN-*).
  - Geoblocking: candidates where IN-home fails but VPN succeeds, with category breakdown.
  - Classes: censorship_class counts and samples from the enriched summary.

### Vantages
- `IN-home` is the default local vantage.
- Additional vantages (e.g., `VPN-UK`, `VPN-EU`) can be collected by running the measurement script with that label; main.py uses the latest run per vantage. Comparison files in `data/vantage_comparison_*_vs_*.csv` are auto-detected by the dashboard.
