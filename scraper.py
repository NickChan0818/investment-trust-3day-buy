#!/usr/bin/env python3
"""
投信連續買進3天篩選器
1. 從 megatime 取得當日投信買超排行榜
2. 進入各股票頁面取得三大法人歷史資料
3. 篩選出投信連續買進 3 天（含今日）的股票
"""

import csv
import re
import sys
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://pchome.megatime.com.tw"
RANK_URL = f"{BASE_URL}/rank/sto4/ock04.html"
REQUEST_DELAY = 0.8   # seconds between requests (be polite)
CONSECUTIVE_DAYS = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8",
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    # Prime cookies by hitting the homepage
    try:
        s.get(BASE_URL, timeout=15)
        time.sleep(0.3)
    except Exception:
        pass
    return s


def safe_get(session: requests.Session, url: str, retries: int = 3) -> requests.Response | None:
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 200:
                return resp
            print(f"    HTTP {resp.status_code} for {url}")
        except requests.RequestException as exc:
            print(f"    Request error ({attempt}/{retries}): {exc}")
        if attempt < retries:
            time.sleep(2 ** attempt)
    return None


# ---------------------------------------------------------------------------
# Ranking page parser
# ---------------------------------------------------------------------------

def fetch_ranking(session: requests.Session) -> list[dict]:
    """
    Scrape the 投信買超 ranking page.
    Returns a list of dicts: {code, name, buy_volume}
    """
    print(f"  GET {RANK_URL}")
    resp = safe_get(session, RANK_URL)
    if resp is None:
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    stocks = []
    seen = set()

    # Links to stock pages look like /stock/sto1/sid2353.html
    for a in soup.find_all("a", href=re.compile(r"/stock/sto\d/sid(\d+)\.html")):
        m = re.search(r"sid(\d+)\.html", a["href"])
        if not m:
            continue
        code = m.group(1)
        if code in seen:
            continue
        seen.add(code)

        name = a.get_text(strip=True)
        buy_volume = _extract_nearby_number(a)
        stocks.append({"code": code, "name": name, "buy_volume": buy_volume})

    return stocks


def _extract_nearby_number(tag) -> int | None:
    """Walk up to the parent <tr> and look for the largest positive integer cell."""
    tr = tag.find_parent("tr")
    if tr is None:
        return None
    best = None
    for td in tr.find_all("td"):
        raw = td.get_text(strip=True).replace(",", "").replace("+", "")
        try:
            v = int(raw)
            if v > 0 and (best is None or v > best):
                best = v
        except ValueError:
            pass
    return best


# ---------------------------------------------------------------------------
# Per-stock 三大法人 history
# ---------------------------------------------------------------------------

# Candidate sub-pages to look for institutional data (most to least likely)
SUBPAGES = ["sto3", "sto1", "sto4"]


def fetch_trust_history(session: requests.Session, code: str) -> list[int]:
    """
    Return list of 投信 net-buy values (positive = buying, negative = selling)
    for recent trading days, newest-first.

    Tries several sub-page URLs and parsing strategies.
    """
    for sto in SUBPAGES:
        url = f"{BASE_URL}/stock/{sto}/sid{code}.html"
        resp = safe_get(session, url)
        if resp is None:
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        values = _parse_trust_net(soup)
        if values:
            return values

        time.sleep(REQUEST_DELAY * 0.5)

    return []


def _parse_trust_net(soup: BeautifulSoup) -> list[int]:
    """
    Try multiple strategies to extract 投信 net buy/sell values from a page.
    Returns values newest-first (matches table row order on most sites).
    """
    # Strategy A: find a <table> whose column headers contain '投信'
    for table in soup.find_all("table"):
        result = _parse_table_for_trust(table)
        if result:
            return result

    # Strategy B: look for a <tr> or <div> labelled '投信' with numbers beside it
    return _parse_labeled_rows(soup)


def _parse_table_for_trust(table) -> list[int]:
    """
    Inside a table, find columns labelled '投信' and extract net values.
    Handles two layouts:
      - Single '投信淨買賣' / '投信買超' column
      - Separate '投信買進' and '投信賣出' columns (net = buy - sell)
    """
    rows = table.find_all("tr")
    if len(rows) < 2:
        return []

    # Collect header text for every column
    header_row = rows[0]
    headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]

    net_col = buy_col = sell_col = None
    for i, h in enumerate(headers):
        if "投信" in h and any(k in h for k in ("淨", "買超", "賣超")):
            net_col = i
        elif "投信" in h and "買" in h:
            buy_col = i
        elif "投信" in h and "賣" in h:
            sell_col = i

    if net_col is None and not (buy_col is not None and sell_col is not None):
        return []

    values = []
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        try:
            if net_col is not None and net_col < len(cells):
                v = _to_int(cells[net_col].get_text(strip=True))
                if v is not None:
                    values.append(v)
            elif buy_col is not None and sell_col is not None:
                b = _to_int(cells[buy_col].get_text(strip=True))
                s = _to_int(cells[sell_col].get_text(strip=True))
                if b is not None and s is not None:
                    values.append(b - s)
        except (IndexError, TypeError):
            continue

    return values


def _parse_labeled_rows(soup: BeautifulSoup) -> list[int]:
    """
    Some pages use <tr> rows where the first cell contains '投信' and subsequent
    cells contain daily numbers (common in horizontal-layout tables).
    """
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        label = cells[0].get_text(strip=True)
        if "投信" not in label:
            continue
        values = []
        for cell in cells[1:]:
            v = _to_int(cell.get_text(strip=True))
            if v is not None:
                values.append(v)
        if values:
            return values

    return []


def _to_int(text: str) -> int | None:
    clean = text.replace(",", "").replace("+", "").replace("張", "").strip()
    if clean in ("", "-", "—", "–"):
        return None
    try:
        return int(clean)
    except ValueError:
        try:
            return int(float(clean))
        except ValueError:
            return None


# ---------------------------------------------------------------------------
# Filter logic
# ---------------------------------------------------------------------------

def is_consecutive_buy(values: list[int], days: int = CONSECUTIVE_DAYS) -> bool:
    """Return True if the first `days` values are all strictly positive."""
    if len(values) < days:
        return False
    return all(v > 0 for v in values[:days])


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def print_results(results: list[dict]) -> None:
    if not results:
        print("\n本日無符合條件的股票（投信連續3天買進）\n")
        return

    print(f"\n{'代碼':<8} {'名稱':<16} {'今日買超(張)':>12} "
          f"{'D1淨':>8} {'D2淨':>8} {'D3淨':>8}")
    print("-" * 68)
    for r in results:
        print(f"{r['code']:<8} {r['name']:<16} {_fmt(r['buy_volume']):>12} "
              f"{_fmt(r['d1_net']):>8} {_fmt(r['d2_net']):>8} {_fmt(r['d3_net']):>8}")


def _fmt(v) -> str:
    if v is None:
        return "-"
    return f"{v:,}"


def save_csv(results: list[dict]) -> str:
    if not results:
        return ""
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    fname = f"trust_3day_buy_{ts}.csv"
    fieldnames = ["code", "name", "buy_volume", "d1_net", "d2_net", "d3_net"]
    with open(fname, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow({k: r.get(k, "") for k in fieldnames})
    return fname


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 68)
    print(f"  投信連續買進 {CONSECUTIVE_DAYS} 天篩選器")
    print(f"  來源: {RANK_URL}")
    print(f"  時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 68)

    session = make_session()

    # Step 1 – ranking
    print("\n[1/3] 取得投信買超排行榜...")
    ranked = fetch_ranking(session)
    if not ranked:
        print("ERROR: 排行榜資料為空。請確認網路連線或網頁結構是否改變。")
        sys.exit(1)
    print(f"      找到 {len(ranked)} 檔股票")

    # Step 2 – per-stock history
    print(f"\n[2/3] 逐一查詢各股票三大法人歷史（共 {len(ranked)} 檔）...")
    results = []
    for i, stock in enumerate(ranked, 1):
        code, name = stock["code"], stock["name"]
        print(f"  ({i:3d}/{len(ranked)}) [{code}] {name} ...", end=" ", flush=True)

        history = fetch_trust_history(session, code)

        if is_consecutive_buy(history, CONSECUTIVE_DAYS):
            results.append({
                "code": code,
                "name": name,
                "buy_volume": stock["buy_volume"],
                "d1_net": history[0] if len(history) > 0 else None,
                "d2_net": history[1] if len(history) > 1 else None,
                "d3_net": history[2] if len(history) > 2 else None,
            })
            print(f"✓  {history[:CONSECUTIVE_DAYS]}")
        else:
            print(f"✗  {history[:CONSECUTIVE_DAYS] if history else '(無資料)'}")

        time.sleep(REQUEST_DELAY)

    # Step 3 – output
    print(f"\n[3/3] 篩選結果 — 符合條件：{len(results)} 檔")
    print("=" * 68)
    print_results(results)

    if results:
        fname = save_csv(results)
        if fname:
            print(f"\n結果已儲存: {fname}")


if __name__ == "__main__":
    main()
