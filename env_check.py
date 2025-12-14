"""
Quick sanity check to confirm required libraries import correctly.
Run from project root: python env_check.py
"""


def main() -> None:
    try:
        import pandas  # type: ignore
        import requests  # type: ignore
        import dns.resolver  # type: ignore
    except Exception as exc:  # pragma: no cover - defensive
        print(f"Import failed: {exc}")
        raise

    print("Imports succeeded.")
    print(f"pandas version: {pandas.__version__}")
    print(f"requests version: {requests.__version__}")
    # dnspython exposes version_info tuple
    version_info = getattr(dns, "__version__", None) or getattr(
        dns.version, "version", "unknown"
    )
    print(f"dnspython version: {version_info}")


if __name__ == "__main__":
    main()
