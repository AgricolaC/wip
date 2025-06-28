"""
fetch.py  ────────────────────────────────────────────────────────────────
Downloads 10-K and 10-Q filings for every S&P 100 company (or
a custom ticker list) and drops them into data/raw/.

Run from repo root:
    python src/fetch.py                  # full S&P 100, 2024+
    python src/fetch.py --tickers AAPL   # toy set
"""

import argparse, time
from pathlib import Path
from typing import List

import pandas as pd
from sec_edgar_downloader import Downloader

# ── constant URLs ───────────────────────────────────────────────────────
SNP100_URL = (
    "https://en.wikipedia.org/wiki/S%26P_100"  # table is stable & scrape-friendly
)

# ── helpers ─────────────────────────────────────────────────────────────
def get_sp100_tickers() -> List[str]:
    """Scrape the Symbol column from the Wikipedia S&P 100 table."""
    tables = pd.read_html(SNP100_URL, match="Symbol")
    return tables[0]["Symbol"].tolist()

def polite_downloader(outdir: Path) -> Downloader:
    """Instantiate the downloader with required contact info and target folder."""
    return Downloader("SecFilingsExplorer", "you@example.com", outdir)


def fetch_forms(
    tickers: List[str],
    form_types=("10-K", "10-Q"),
    after="2024-01-01",
    outdir="data/raw",
    sleep=0.3,
) -> None:
    """Grab filings, copy them to outdir (kept out of Git)."""
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)

    dl = polite_downloader(out)    
    for tic in tickers:
        for ftype in form_types:
            dl.get(ftype, tic, after=after)   # returns an int we can ignore
        time.sleep(sleep)  # respect SEC bandwidth guidance
    print("Download complete –", len(list(out.glob('*'))), "files in", out)

# ── CLI ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=None,
        help="Space-separated list of tickers (defaults to full S&P 100)",
    )
    parser.add_argument("--after", default="2024-01-01", help="YYYY-MM-DD cut-off")
    args = parser.parse_args()

    ticker_list = args.tickers or get_sp100_tickers()
    fetch_forms(ticker_list, after=args.after)
