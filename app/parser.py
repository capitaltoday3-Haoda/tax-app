import re
from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Tuple

import pdfplumber


@dataclass
class Trade:
    symbol: str
    currency: str
    trade_date: date
    side: str  # "BUY" or "SELL"
    qty: float
    price: float
    source: str


@dataclass
class Holding:
    symbol: str
    currency: str
    qty: float
    name: str


def _parse_number(token: str) -> Optional[float]:
    token = token.replace(",", "").strip()
    if not token:
        return None
    neg = token.startswith("(") and token.endswith(")")
    if neg:
        token = token[1:-1]
    try:
        val = float(token)
    except ValueError:
        return None
    return -val if neg else val


def _parse_date(token: str) -> Optional[date]:
    try:
        y, m, d = token.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


def extract_text_pages(pdf_path: str) -> List[str]:
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or "")
    return pages


def parse_statement_month(text: str) -> Optional[Tuple[int, int]]:
    match = re.search(r"月结单\s*\((\d{4})-(\d{2})\)", text)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _extract_section_lines(text: str, start_marker: str, end_markers: List[str]) -> List[str]:
    start_idx = text.find(start_marker)
    if start_idx < 0:
        return []
    sub = text[start_idx + len(start_marker) :]
    end_idx = len(sub)
    for marker in end_markers:
        idx = sub.find(marker)
        if idx >= 0:
            end_idx = min(end_idx, idx)
    section = sub[:end_idx]
    return [line.strip() for line in section.splitlines() if line.strip()]


def _infer_currency_from_code(code: str) -> Optional[str]:
    if code.endswith(":HK"):
        return "HKD"
    if code.endswith(":US"):
        return "USD"
    return None


def _normalize_symbol(code: str) -> str:
    sym = code.split(":")[0]
    return sym.replace("*", "")


def parse_trades(text: str) -> List[Trade]:
    trades: List[Trade] = []

    # 成交单据
    trade_lines = _extract_section_lines(text, "成交单据", ["户口变动", "持货结存"])
    for line in trade_lines:
        if not re.match(r"^\d{8,}\s", line):
            continue
        parts = line.split()
        if len(parts) < 9:
            continue
        ref = parts[0]
        settle = _parse_date(parts[1])
        side_cn = parts[2]
        code = parts[3]
        price = _parse_number(parts[4])
        qty = _parse_number(parts[5])
        if not settle or price is None or qty is None:
            continue
        currency = _infer_currency_from_code(code)
        if currency is None:
            continue
        if ":FUND" in code:
            continue
        side = "BUY" if side_cn == "买入" else "SELL" if side_cn == "沽出" else None
        if side is None:
            continue
        trades.append(
            Trade(
                symbol=_normalize_symbol(code),
                currency=currency,
                trade_date=settle,
                side=side,
                qty=abs(qty),
                price=price,
                source=f"成交单据:{ref}",
            )
        )

    # 户口变动 - 买卖交易
    account_lines = _extract_section_lines(text, "户口变动", ["持货结存"])
    pattern = re.compile(
        r"^(?P<ref>\d{8,})\s+(?P<settle>\d{4}-\d{2}-\d{2})\s+"
        r"(?P<trade>\d{4}-\d{2}-\d{2})\s+买卖交易\s+"
        r"(?P<side>买入|沽出)\s+(?P<code>[A-Z0-9]+:(?:HK|US))\s+"
        r".*?@(?P<price>[\d.]+)\s+(?P<qty>[\d,().-]+)"
    )
    for line in account_lines:
        if "买卖交易" not in line:
            continue
        match = pattern.search(line)
        if not match:
            continue
        trade_date = _parse_date(match.group("trade"))
        if not trade_date:
            continue
        side = "BUY" if match.group("side") == "买入" else "SELL"
        code = match.group("code")
        currency = _infer_currency_from_code(code)
        if currency is None:
            continue
        price = _parse_number(match.group("price"))
        qty = _parse_number(match.group("qty"))
        if price is None or qty is None:
            continue
        trades.append(
            Trade(
                symbol=_normalize_symbol(code),
                currency=currency,
                trade_date=trade_date,
                side=side,
                qty=abs(qty),
                price=price,
                source=f"户口变动:{match.group('ref')}",
            )
        )

    return trades


def parse_holdings(text: str) -> List[Holding]:
    holdings: List[Holding] = []
    section_lines = _extract_section_lines(text, "持货结存", ["股票借贷资料", "重要提示"])
    currency = None
    for line in section_lines:
        if "HK - HONG KONG STOCK" in line:
            currency = "HKD"
            continue
        if "US - U.S. STOCK" in line:
            currency = "USD"
            continue
        if "FUND - FUND" in line:
            currency = None
            continue
        if currency not in ("HKD", "USD"):
            continue
        if not re.match(r"^[A-Z0-9]", line):
            continue
        tokens = line.split()
        if len(tokens) < 3:
            continue
        code = tokens[0].replace("*", "")
        num_idx = None
        for i in range(1, len(tokens)):
            if _parse_number(tokens[i]) is not None:
                num_idx = i
                break
        if num_idx is None:
            continue
        name = " ".join(tokens[1:num_idx]).strip()
        nums = [_parse_number(t) for t in tokens[num_idx:] if _parse_number(t) is not None]
        if len(nums) < 4:
            continue
        net_qty = nums[3]
        if net_qty is None:
            continue
        holdings.append(
            Holding(symbol=code, currency=currency, qty=float(net_qty), name=name)
        )
    return holdings


def parse_pdf(pdf_path: str) -> Tuple[Optional[Tuple[int, int]], List[Trade], List[Holding]]:
    pages = extract_text_pages(pdf_path)
    full_text = "\n".join(pages)
    month = parse_statement_month(full_text)
    trades = parse_trades(full_text)
    holdings = parse_holdings(full_text)
    return month, trades, holdings
