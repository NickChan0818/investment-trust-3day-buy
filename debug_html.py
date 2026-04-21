#!/usr/bin/env python3
"""
Debug helper: fetch raw HTML from megatime and save to disk so you can
inspect the actual page structure and tune the parser in scraper.py.

Usage:
    python3 debug_html.py                  # saves ranking page + first 3 stocks
    python3 debug_html.py --code 2330      # saves a specific stock page
"""

import argparse
import re
import time
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from scraper import BASE_URL, RANK_URL, SUBPAGES, make_session, safe_get

OUT_DIR = Path("debug_html")


def dump(name: str, html: str) -> None:
    OUT_DIR.mkdir(exist_ok=True)
    p = OUT_DIR / f"{name}.html"
    p.write_text(html, encoding="utf-8")
    print(f"  Saved: {p}")


def summarise(html: str, label: str) -> None:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    print(f"\n  [{label}] {len(tables)} tables found")
    for i, t in enumerate(tables[:5]):
        headers = [th.get_text(strip=True) for th in t.find_all(["th"])][:8]
        rows = len(t.find_all("tr"))
        print(f"    table[{i}]: {rows} rows, headers={headers}")

    trust_hits = [tag for tag in soup.find_all(string=re.compile("投信"))]
    print(f"  '投信' appears {len(trust_hits)} times in text nodes")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--code", help="Stock code to inspect (e.g. 2330)")
    args = parser.parse_args()

    session = make_session()

    if args.code:
        for sto in SUBPAGES:
            url = f"{BASE_URL}/stock/{sto}/sid{args.code}.html"
            print(f"Fetching {url} ...")
            resp = safe_get(session, url)
            if resp is None:
                print("  FAILED")
                continue
            name = f"stock_{args.code}_{sto}"
            dump(name, resp.text)
            summarise(resp.text, name)
            time.sleep(1)
    else:
        # Dump ranking page
        print(f"Fetching {RANK_URL} ...")
        resp = safe_get(session, RANK_URL)
        if resp is None:
            print("Failed to fetch ranking page.")
            sys.exit(1)
        dump("ranking", resp.text)
        summarise(resp.text, "ranking")

        # Extract first 3 stock codes
        soup = BeautifulSoup(resp.text, "lxml")
        codes = []
        for a in soup.find_all("a", href=re.compile(r"/stock/sto\d/sid(\d+)\.html")):
            m = re.search(r"sid(\d+)\.html", a["href"])
            if m and m.group(1) not in codes:
                codes.append(m.group(1))
            if len(codes) >= 3:
                break

        print(f"\nFirst 3 stock codes found: {codes}")
        for code in codes:
            for sto in SUBPAGES[:2]:
                url = f"{BASE_URL}/stock/{sto}/sid{code}.html"
                print(f"Fetching {url} ...")
                r = safe_get(session, url)
                if r:
                    name = f"stock_{code}_{sto}"
                    dump(name, r.text)
                    summarise(r.text, name)
                time.sleep(1)

    print(f"\nDone. Open the files in {OUT_DIR}/ to inspect the HTML structure.")


if __name__ == "__main__":
    main()
