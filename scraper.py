#!/usr/bin/env python3
"""
投信連續買進3天篩選器
1. 從 megatime 取得當日投信買超排行榜
2. 進入各股票頁面取得三大法人歷史資料
3. 篩選出投信連續買進 3 天（含今日）的股票
"""

import re
import sys
import time
from datetime import datetime

import urllib3
import requests
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://pchome.megatime.com.tw"
RANK_URL = f"{BASE_URL}/rank/sto4/ock04.html"
REQUEST_DELAY = 0.8   # seconds between requests (be polite)
CONSECUTIVE_DAYS = 3
TOP_N = 30  # 取排行榜前幾名

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    s.verify = False  # 目標網站憑證缺少 Subject Key Identifier，停用 SSL 驗證
    try:
        s.headers.update({"Sec-Fetch-Site": "none"})
        s.get(BASE_URL, timeout=15)
        time.sleep(0.5)
    except Exception:
        pass
    finally:
        s.headers.update({
            "Referer": BASE_URL + "/",
            "Sec-Fetch-Site": "same-origin",
        })
    return s


def _needs_is_check(resp: requests.Response) -> bool:
    """偵測網站回傳的 is_check 表單重導向（JS 自動提交）。"""
    return len(resp.text) < 1000 and "is_check" in resp.text and "submit_form" in resp.text


def safe_get(session: requests.Session, url: str, retries: int = 3,
             extra_headers: dict | None = None) -> requests.Response | None:
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=20, headers=extra_headers or {})
            if resp.status_code == 200:
                if _needs_is_check(resp):
                    # 模擬 JS 表單自動提交
                    resp = session.post(url, data={"is_check": "1"}, timeout=20)
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
    resp = safe_get(session, RANK_URL, extra_headers={"Referer": BASE_URL + "/rank/"})
    if resp is None:
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    stocks = []
    seen = set()

    # Links to stock pages look like /stock/sid2353.html
    for a in soup.find_all("a", href=re.compile(r"/stock/sid(\d+)\.html")):
        m = re.search(r"sid(\d+)\.html", a["href"])
        if not m:
            continue
        code = m.group(1)
        if code in seen:
            continue
        seen.add(code)

        if code.startswith("00"):  # 排除 ETF
            continue
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


TWSE_API = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
YAHOO_API = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"


def fetch_prices(code: str, n: int = CONSECUTIVE_DAYS) -> list[float]:
    """
    取得近 n 個交易日的收盤價（由新到舊）。
    先試上市 TWSE；若查無資料再試上櫃 Yahoo Finance (.TWO)。
    """
    today = datetime.now()
    prices: list[float] = []

    # --- 上市：TWSE 月查詢（一次抓整月） ---
    for delta_month in range(2):
        month = today.month - delta_month
        year = today.year
        if month <= 0:
            month += 12
            year -= 1
        date_str = f"{year}{month:02d}01"
        try:
            resp = requests.get(
                TWSE_API,
                params={"date": date_str, "stockNo": code, "response": "json"},
                verify=False,
                timeout=15,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            if data.get("stat") != "OK":
                continue
            fields = data.get("fields", [])
            close_idx = next((i for i, f in enumerate(fields) if "收盤" in f), None)
            if close_idx is None:
                continue
            rows = data.get("data", [])
            for row in reversed(rows):
                try:
                    prices.append(float(row[close_idx].replace(",", "")))
                except (ValueError, IndexError):
                    pass
                if len(prices) >= n:
                    return prices[:n]
        except Exception:
            pass
    if prices:
        return prices[:n]

    # --- 上櫃：Yahoo Finance (.TWO) ---
    try:
        resp = requests.get(
            YAHOO_API.format(ticker=f"{code}.TWO"),
            params={"range": f"{n + 7}d", "interval": "1d"},
            verify=False,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        if resp.status_code == 200:
            result = resp.json().get("chart", {}).get("result", [])
            if result:
                closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
                # closes 由舊到新；取末尾 n 筆，再反轉成新到舊
                valid = [c for c in closes if c is not None]
                prices = list(reversed(valid[-n:]))
    except Exception:
        pass

    return prices[:n]



# ---------------------------------------------------------------------------
# Per-stock 三大法人 history
# ---------------------------------------------------------------------------

# Candidate sub-pages to look for institutional data (most to least likely)
# sto1 = 籌碼分析（含投信持股逐日表格）
SUBPAGES = ["sto1", "sto3", "sto4"]


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
    # Strategy A: find the table that contains '投信持股' heading
    # (sto1 籌碼分析 page: the span is inside the same table as the data)
    for heading in soup.find_all(string=re.compile("投信持股")):
        table = heading.find_parent("table")
        if table:
            result = _parse_table_for_trust(table, require_trust_label=False)
            if result:
                return result

    # Strategy B: find a <table> whose column headers contain '投信'
    for table in soup.find_all("table"):
        result = _parse_table_for_trust(table, require_trust_label=True)
        if result:
            return result

    # Strategy C: look for a <tr> or <div> labelled '投信' with numbers beside it
    return _parse_labeled_rows(soup)


def _parse_table_for_trust(table, require_trust_label: bool = False) -> list[int]:
    """
    Inside a table, find columns labelled '投信' and extract net values.
    Handles two layouts:
      - Single '投信淨買賣' / '投信買超' / '買賣超張數' column
      - Separate '投信買進' and '投信賣出' columns (net = buy - sell)
    When require_trust_label=False, also matches tables whose parent section
    is labelled '投信持股' (e.g. sto1 籌碼分析 page).
    """
    rows = table.find_all("tr")
    if len(rows) < 2:
        return []

    # Try each row as the header (handles tables where row[0] is a title row)
    header_row_idx = None
    net_col = buy_col = sell_col = None
    for row_idx, row in enumerate(rows):
        cells = row.find_all(["th", "td"])
        headers = [c.get_text(strip=True) for c in cells]
        n = b = s = None
        for i, h in enumerate(headers):
            if "投信" in h and any(k in h for k in ("淨", "買超", "賣超")):
                n = i
            elif "投信" in h and "買" in h:
                b = i
            elif "投信" in h and "賣" in h:
                s = i
            elif not require_trust_label and "買賣超" in h and n is None:
                n = i
        if n is not None or (b is not None and s is not None):
            header_row_idx = row_idx
            net_col, buy_col, sell_col = n, b, s
            break

    if header_row_idx is None:
        return []

    values = []
    for row in rows[header_row_idx + 1:]:
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

def _trading_dates(n: int) -> list[str]:
    """回傳最近 n 個交易日（週一到週五）的日期字串，由新到舊。"""
    from datetime import timedelta
    dates: list[str] = []
    d = datetime.now().date()
    while len(dates) < n:
        if d.weekday() < 5:  # 0=Mon, 4=Fri
            dates.append(d.strftime("%m/%d"))
        d -= timedelta(days=1)
    return dates


def print_results(results: list[dict]) -> None:
    if not results:
        print("\n本日無符合條件的股票（投信連續3天買進）\n")
        return

    td = _trading_dates(CONSECUTIVE_DAYS)
    h1, h2, h3 = f"D1({td[0]})", f"D2({td[1]})", f"D3({td[2]})"
    p1w = max(len(h1), 6)
    p2w = max(len(h2), 6)
    p3w = max(len(h3), 6)

    print()
    print(f"| 代碼 | 名稱 | {h1} | {h2} | {h3} | 收盤({td[0]}) | 收盤({td[1]}) | 收盤({td[2]}) |")
    print(f"|------|------|{'-'*(p1w+1)}:|{'-'*(p2w+1)}:|{'-'*(p3w+1)}:|-------:|-------:|-------:|")
    for r in results:
        print(f"| {r['code']} | {r['name']} "
              f"| {_fmt(r['d1_net'])} | {_fmt(r['d2_net'])} | {_fmt(r['d3_net'])} "
              f"| {_fmtprice(r.get('p1'), r.get('p2'))} | {_fmtprice(r.get('p2'), r.get('p3'))} | {_fmtf(r.get('p3'))} |")


def _fmt(v) -> str:
    if v is None:
        return "-"
    return f"{v:,}"


def _fmtf(v) -> str:
    if v is None:
        return "-"
    return f"{v:.1f}"


def _fmtprice(v, prev) -> str:
    """格式化收盤價，附帶與前一日的漲跌幅。"""
    if v is None:
        return "-"
    s = f"{v:.1f}"
    if prev is not None and prev != 0:
        pct = (v - prev) / prev * 100
        sign = "+" if pct >= 0 else ""
        s += f"({sign}{pct:.0f}%)"
    return s


def save_csv(results: list[dict]) -> str:
    if not results:
        return ""
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    fname = f"trust_3day_buy_{ts}.md"
    td = _trading_dates(CONSECUTIVE_DAYS)
    h1, h2, h3 = f"D1({td[0]})", f"D2({td[1]})", f"D3({td[2]})"
    lines = [
        f"# 投信連續買進 {CONSECUTIVE_DAYS} 天篩選結果",
        f"",
        f"資料日期：{datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"來源：{RANK_URL}",
        f"",
        f"| 代碼 | 名稱 | {h1} | {h2} | {h3} | 收盤({td[0]}) | 收盤({td[1]}) | 收盤({td[2]}) |",
        f"|------|------|{'-'*(len(h1)+1)}:|{'-'*(len(h2)+1)}:|{'-'*(len(h3)+1)}:|-------:|-------:|-------:|",
    ]
    for r in results:
        lines.append(
            f"| {r['code']} | {r['name']} "
            f"| {_fmt(r.get('d1_net'))} | {_fmt(r.get('d2_net'))} | {_fmt(r.get('d3_net'))} "
            f"| {_fmtprice(r.get('p1'), r.get('p2'))} | {_fmtprice(r.get('p2'), r.get('p3'))} | {_fmtf(r.get('p3'))} |"
        )
    with open(fname, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
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
    ranked = fetch_ranking(session)[:TOP_N]
    if not ranked:
        print("ERROR: 排行榜資料為空。請確認網路連線或網頁結構是否改變。")
        sys.exit(1)
    print(f"      找到 {len(ranked)} 檔股票（取前{TOP_N}）")

    # Step 2 – per-stock history
    print(f"\n[2/3] 逐一查詢各股票三大法人歷史（共 {len(ranked)} 檔）...")
    results = []
    for i, stock in enumerate(ranked, 1):
        code, name = stock["code"], stock["name"]
        print(f"  ({i:3d}/{len(ranked)}) [{code}] {name} ...", end=" ", flush=True)

        history = fetch_trust_history(session, code)

        if is_consecutive_buy(history, CONSECUTIVE_DAYS):
            prices = fetch_prices(code)
            results.append({
                "code": code,
                "name": name,
                "buy_volume": stock["buy_volume"],
                "d1_net": history[0] if len(history) > 0 else None,
                "d2_net": history[1] if len(history) > 1 else None,
                "d3_net": history[2] if len(history) > 2 else None,
                "p1": prices[0] if len(prices) > 0 else None,
                "p2": prices[1] if len(prices) > 1 else None,
                "p3": prices[2] if len(prices) > 2 else None,
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
