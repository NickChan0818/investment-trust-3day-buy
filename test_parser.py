#!/usr/bin/env python3
"""
Unit tests for the core parsing and filter logic in scraper.py.
Run with: python3 test_parser.py
"""

import unittest
from unittest.mock import MagicMock, patch

import requests
from bs4 import BeautifulSoup
from scraper import (
    _parse_table_for_trust,
    _parse_labeled_rows,
    _to_int,
    is_consecutive_buy,
    safe_get,
)


# ---------------------------------------------------------------------------
# _to_int
# ---------------------------------------------------------------------------
class TestToInt(unittest.TestCase):
    def test_plain_number(self):
        self.assertEqual(_to_int("1234"), 1234)

    def test_number_with_commas(self):
        self.assertEqual(_to_int("1,234"), 1234)

    def test_positive_sign(self):
        self.assertEqual(_to_int("+500"), 500)

    def test_negative(self):
        self.assertEqual(_to_int("-300"), -300)

    def test_張_suffix(self):
        self.assertEqual(_to_int("200張"), 200)

    def test_empty(self):
        self.assertIsNone(_to_int(""))

    def test_dash(self):
        self.assertIsNone(_to_int("-"))

    def test_em_dash(self):
        self.assertIsNone(_to_int("—"))

    def test_float_string(self):
        self.assertEqual(_to_int("1.5"), 1)


# ---------------------------------------------------------------------------
# _parse_table_for_trust  (net-column layout)
# ---------------------------------------------------------------------------
NET_COL_HTML = """
<table>
  <tr><th>日期</th><th>外資淨買賣</th><th>投信淨買賣</th><th>自營商淨買賣</th></tr>
  <tr><td>2024/04/18</td><td>+500</td><td>+120</td><td>-30</td></tr>
  <tr><td>2024/04/17</td><td>+200</td><td>+80</td><td>+10</td></tr>
  <tr><td>2024/04/16</td><td>-100</td><td>+50</td><td>-20</td></tr>
  <tr><td>2024/04/15</td><td>+300</td><td>-15</td><td>+5</td></tr>
</table>
"""

# ---------------------------------------------------------------------------
# _parse_table_for_trust  (separate buy/sell columns)
# ---------------------------------------------------------------------------
BUY_SELL_COL_HTML = """
<table>
  <tr><th>日期</th><th>投信買進</th><th>投信賣出</th></tr>
  <tr><td>2024/04/18</td><td>1,000</td><td>880</td></tr>
  <tr><td>2024/04/17</td><td>900</td><td>820</td></tr>
  <tr><td>2024/04/16</td><td>400</td><td>350</td></tr>
</table>
"""

# ---------------------------------------------------------------------------
# _parse_labeled_rows  (horizontal layout: first cell is label)
# ---------------------------------------------------------------------------
HORIZONTAL_HTML = """
<table>
  <tr><td>外資</td><td>+500</td><td>+200</td><td>-100</td></tr>
  <tr><td>投信</td><td>+120</td><td>+80</td><td>+50</td></tr>
  <tr><td>自營商</td><td>-30</td><td>+10</td><td>-20</td></tr>
</table>
"""

NO_TRUST_HTML = """
<table>
  <tr><th>日期</th><th>外資淨</th><th>自營商淨</th></tr>
  <tr><td>2024/04/18</td><td>+500</td><td>-30</td></tr>
</table>
"""


class TestParseTableForTrust(unittest.TestCase):
    def _table(self, html):
        return BeautifulSoup(html, "lxml").find("table")

    def test_net_column(self):
        result = _parse_table_for_trust(self._table(NET_COL_HTML))
        self.assertEqual(result, [120, 80, 50, -15])

    def test_buy_sell_columns(self):
        result = _parse_table_for_trust(self._table(BUY_SELL_COL_HTML))
        self.assertEqual(result, [120, 80, 50])  # 1000-880, 900-820, 400-350

    def test_no_trust_column(self):
        result = _parse_table_for_trust(self._table(NO_TRUST_HTML))
        self.assertEqual(result, [])


class TestParseLabeledRows(unittest.TestCase):
    def test_horizontal_layout(self):
        soup = BeautifulSoup(HORIZONTAL_HTML, "lxml")
        result = _parse_labeled_rows(soup)
        self.assertEqual(result, [120, 80, 50])

    def test_no_match(self):
        soup = BeautifulSoup("<table><tr><td>外資</td><td>100</td></tr></table>", "lxml")
        result = _parse_labeled_rows(soup)
        self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# is_consecutive_buy
# ---------------------------------------------------------------------------
class TestIsConsecutiveBuy(unittest.TestCase):
    def test_three_positive(self):
        self.assertTrue(is_consecutive_buy([100, 50, 20], 3))

    def test_mixed(self):
        self.assertFalse(is_consecutive_buy([100, -5, 20], 3))

    def test_first_two_positive(self):
        self.assertFalse(is_consecutive_buy([100, 50], 3))  # too short

    def test_exact_boundary(self):
        self.assertTrue(is_consecutive_buy([1, 1, 1, -100], 3))

    def test_zero_not_buying(self):
        self.assertFalse(is_consecutive_buy([100, 0, 50], 3))

    def test_empty(self):
        self.assertFalse(is_consecutive_buy([], 3))


class TestSafeGet(unittest.TestCase):
    def test_uses_empty_headers_when_extra_headers_omitted(self):
        session = MagicMock()
        response = MagicMock(status_code=200)
        session.get.return_value = response

        result = safe_get(session, "https://example.com")

        self.assertIs(result, response)
        session.get.assert_called_once_with(
            "https://example.com",
            timeout=20,
            headers={},
        )

    def test_passes_extra_headers_to_session_get(self):
        session = MagicMock()
        response = MagicMock(status_code=200)
        session.get.return_value = response
        extra_headers = {"Referer": "https://example.com/rank/"}

        result = safe_get(session, "https://example.com/page", extra_headers=extra_headers)

        self.assertIs(result, response)
        session.get.assert_called_once_with(
            "https://example.com/page",
            timeout=20,
            headers=extra_headers,
        )

    @patch("scraper.time.sleep")
    def test_retries_after_request_exception_until_success(self, mock_sleep):
        session = MagicMock()
        success_response = MagicMock(status_code=200)
        session.get.side_effect = [
            requests.RequestException("temporary failure"),
            success_response,
        ]

        result = safe_get(session, "https://example.com", retries=3)

        self.assertIs(result, success_response)
        self.assertEqual(session.get.call_count, 2)
        self.assertEqual(mock_sleep.call_args_list, [unittest.mock.call(2)])


if __name__ == "__main__":
    unittest.main(verbosity=2)
