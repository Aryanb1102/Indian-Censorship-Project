## India Censorship Measurement Project

This repo measures connectivity from India to ~100 domains (news, platforms, government, NGOs), combines local measurements with OONI web connectivity data, and classifies domains into censorship-related categories.

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
- Full local run (single vantage, e.g. home):
```
python main.py --do-measure --vantage IN-home --do-ooni --do-merge --do-analyze --do-classify --print-summary
```
- After collecting a VPN vantage (e.g. VPN-EU), run comparison + classify + summary:
```
python main.py --do-vantage-compare --vantage IN-home --remote-vantage VPN-EU --do-classify --print-summary
```
- You can mix flags as needed; `--vantage` controls both measurement and analysis flags.

### Key outputs
- Raw measurements: `data/raw_measurements.csv` (one row per domain per run, with `vantage` and `http_body_snippet`).
- OONI ingest: `data/ooni_india_webconnectivity_clean.csv`.
- Local+OONI merge: `data/domain_level_summary.csv`.
- Vantage comparison: `data/vantage_comparison_<local>_vs_<remote>.csv`.
- Enriched classification: `data/domain_level_summary_enriched.csv`.
- Figures and summaries: `figures/http_outcomes_by_category.png`, `data/http_outcomes_by_category.csv`, `data/tls_issuers_*.csv`, `data/dns_mismatch_local_vs_public.csv`.

### Dashboard
- Launch Streamlit dashboard:
```
streamlit run app/dashboard.py --server.headless true --server.port 8501 --server.address 127.0.0.1
```
- Shows filters by category/vantage/run, HTTP outcomes, TLS issuers, and problematic domains.

### Vantages
- `IN-home` is the default local vantage.
- Additional vantages (e.g., `VPN-EU`) can be collected by running the measurement script with that label; main.py uses the latest run per vantage.
