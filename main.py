"""
Orchestrator for the censorship measurement project.

Example:
python main.py --do-measure --vantage IN-home --do-ooni --do-merge --do-analyze --do-classify --print-summary
"""

import argparse
import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).parent
NOTEBOOKS_DIR = ROOT / "notebooks"


def load_module(path: Path, name: str):
    """Dynamically load a module from a path with a safe name."""
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore
    return module


def run_measure(vantage: str):
    mod = load_module(NOTEBOOKS_DIR / "02_measure_connectivity.py", "measure_connectivity")
    mod.main(vantage)  # type: ignore[attr-defined]


def run_ooni():
    mod = load_module(NOTEBOOKS_DIR / "10_ingest_ooni.py", "ingest_ooni")
    mod.main()  # type: ignore[attr-defined]


def run_merge():
    mod = load_module(NOTEBOOKS_DIR / "20_merge_local_and_ooni.py", "merge_local_ooni")
    mod.main()  # type: ignore[attr-defined]


def run_analyze(vantage: str | None):
    mod = load_module(NOTEBOOKS_DIR / "03_explore_measurements.py", "explore_measurements")
    mod.main(vantage)  # type: ignore[attr-defined]


def run_vantage_compare(local_vantage: str, remote_vantage: str):
    mod = load_module(NOTEBOOKS_DIR / "25_compare_vantages.py", "compare_vantages")
    try:
        df_comp = mod.compare_vantages(local_vantage, remote_vantage)  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - runtime guard
        print(f"Vantage comparison failed: {exc}")
        return
    out_path = Path("data") / f"vantage_comparison_{local_vantage}_vs_{remote_vantage}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_comp.to_csv(out_path, index=False)
    print(f"Saved comparison to {out_path}")
    mod.print_summary(df_comp)  # type: ignore[attr-defined]


def run_classify(comp_file: str | None, local_vantage: str):
    mod = load_module(NOTEBOOKS_DIR / "30_classify_domains.py", "classify_domains")
    argv = []
    if comp_file:
        argv.extend(["--vantage-comparison-file", comp_file])
    if local_vantage:
        argv.extend(["--local-vantage", local_vantage])
    mod.main(argv)  # type: ignore[attr-defined]


def print_summary():
    path = Path("data/domain_level_summary_enriched.csv")
    if not path.exists():
        print("Summary file not found; run classification first.")
        return
    import pandas as pd

    df = pd.read_csv(path)
    print("Domains per censorship_class:")
    print(df["censorship_class"].value_counts())
    non_normal = df[df["censorship_class"] != "normal"]
    if non_normal.empty:
        print("No non-normal domains found.")
    else:
        cols = ["domain", "category", "censorship_class"]
        print("\nTop 10 non-normal domains:")
        print(non_normal[cols].head(10).to_string(index=False))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run censorship measurement pipeline.")
    parser.add_argument("--do-measure", action="store_true", help="Run measurement pipeline.")
    parser.add_argument("--vantage", default="IN-home", help="Vantage label for measurement/analysis.")
    parser.add_argument("--do-ooni", action="store_true", help="Fetch OONI data.")
    parser.add_argument("--do-merge", action="store_true", help="Merge local data with OONI.")
    parser.add_argument("--do-analyze", action="store_true", help="Run analysis summaries.")
    parser.add_argument(
        "--do-vantage-compare", action="store_true", help="Compare two vantages (latest runs)."
    )
    parser.add_argument("--remote-vantage", default="VPN-EU", help="Remote vantage label for comparison.")
    parser.add_argument("--do-classify", action="store_true", help="Enrich domain summaries with classes.")
    parser.add_argument("--print-summary", action="store_true", help="Print final classification summary.")
    parser.add_argument(
        "--comp-file",
        help="Optional explicit path to a vantage comparison file for classification.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.do_measure:
        run_measure(args.vantage)
    if args.do_ooni:
        run_ooni()
    if args.do_merge:
        run_merge()
    if args.do_analyze:
        run_analyze(args.vantage)
    if args.do_vantage_compare:
        run_vantage_compare(args.vantage, args.remote_vantage)
    if args.do_classify:
        run_classify(args.comp_file, args.vantage)
    if args.print_summary:
        print_summary()


if __name__ == "__main__":
    main()
