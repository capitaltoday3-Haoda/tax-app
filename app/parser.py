import re
from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Tuple

import pdfplumber


@dataclass
class Trade:
    account_id: str
    symbol: str
    name: str | None
    currency: str
    trade_date: date
    side: str  # "BUY" or "SELL"
    qty: float
    price: float
    source: str


@dataclass
class Holding:
    account_id: str
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


def _parse_date(token: str, fmt: str = "%Y-%m-%d") -> Optional[date]:
    try:
        if fmt == "%Y/%m/%d":
            y, m, d = token.split("/")
        else:
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


def _huatai_account_id(text: str) -> str:
    match = re.search(r"客户户口\s*:\s*(\d+)", text)
    if match:
        return f"HTSC-{match.group(1)}"
    return "HTSC-UNKNOWN"


def parse_huatai(text: str) -> Tuple[Optional[Tuple[int, int]], List[Trade], List[Holding], str]:
    account_id = _huatai_account_id(text)
    month = parse_statement_month(text)
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
        side_map = {
            "买入": "BUY",
            "买入开仓": "BUY",
            "卖出": "SELL",
            "沽出": "SELL",
            "卖出平仓": "SELL",
        }
        side = side_map.get(side_cn)
        if side is None:
            continue
        trades.append(
            Trade(
                account_id=account_id,
                symbol=_normalize_symbol(code),
                name=None,
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
        r"(?P<side>买入|沽出|卖出平仓|买入开仓|卖出)\s+(?P<code>[A-Z0-9]+:(?:HK|US))\s+"
        r"(?P<name>.+?)\s+@(?P<price>[\d.]+)\s+(?P<qty>[\d,().-]+)"
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
        side = "BUY" if match.group("side") in ("买入", "买入开仓") else "SELL"
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
                account_id=account_id,
                symbol=_normalize_symbol(code),
                name=match.group("name").strip() if match.group("name") else None,
                currency=currency,
                trade_date=trade_date,
                side=side,
                qty=abs(qty),
                price=price,
                source=f"户口变动:{match.group('ref')}",
            )
        )

    # 户口变动 - 现货存入（新股中签等同买入）
    ipo_pattern = re.compile(
        r"^(?P<ref>\d{8,})\s+(?P<settle>\d{4}-\d{2}-\d{2})\s+现货存入\s+"
        r"(?P<code>\d{4,5})\s+(?P<name>.+?)\s+.*?@(?P<price>[\d.]+)\s+"
        r"(?P<qty>[\d,]+)"
    )
    for line in account_lines:
        if "现货存入" not in line:
            continue
        if "Successful IPO" not in line and "新股" not in line:
            continue
        m = ipo_pattern.search(line)
        if not m:
            continue
        trade_date = _parse_date(m.group("settle"))
        if not trade_date:
            continue
        code = m.group("code")
        name = m.group("name").strip()
        price = _parse_number(m.group("price"))
        qty = _parse_number(m.group("qty"))
        if price is None or qty is None:
            continue
        # Treat as HKD stock buy
        trades.append(
            Trade(
                account_id=account_id,
                symbol=code,
                name=name,
                currency="HKD",
                trade_date=trade_date,
                side="BUY",
                qty=abs(qty),
                price=price,
                source=f"现货存入:{m.group('ref')}",
            )
        )

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
        # Filter to stocks only (skip options)
        if re.search(r"\d{6,}[CP]\d{4,}", code):
            continue
        holdings.append(
            Holding(account_id=account_id, symbol=code, currency=currency, qty=float(net_qty), name=name)
        )

    return month, trades, holdings, account_id


def _normalize_duplicated(text: str) -> str:
    if not text:
        return text
    out = []
    prev = ""
    for ch in text:
        if ch == prev and not ch.isdigit() and ch not in ".,-()/":
            continue
        out.append(ch)
        prev = ch
    return "".join(out)


def _merge_wrapped_lines(lines: List[str]) -> List[str]:
    merged: List[str] = []
    buffer = ""
    for line in lines:
        if buffer:
            buffer = buffer + line
            if buffer.count("(") <= buffer.count(")"):
                merged.append(buffer)
                buffer = ""
            continue
        if re.search(r"(買入|賣出|賣出平倉)\s+[A-Z0-9.]+\([^)]*$", line):
            buffer = line
            continue
        merged.append(line)
    if buffer:
        merged.append(buffer)
    return merged


def _futu_account_id(text: str) -> str:
    match = re.search(r"賬戶號碼[:：]?\s*(\d{6,})", text)
    if match:
        return f"FUTU-{match.group(1)}"
    match = re.search(r"帳戶號碼[:：]?\s*(\d{6,})", text)
    if match:
        return f"FUTU-{match.group(1)}"
    return "FUTU-UNKNOWN"


def parse_futu(text: str) -> Tuple[Optional[Tuple[int, int]], List[Trade], List[Holding], str]:
    text = _normalize_duplicated(text)
    account_id = _futu_account_id(text)

    month = None
    match = re.search(r"(\d{4})/(\d{2})", text)
    if match:
        month = (int(match.group(1)), int(match.group(2)))

    trades: List[Trade] = []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    lines = _merge_wrapped_lines(lines)

    in_trades = False
    current_symbol = None
    current_name = None
    current_side = None
    for line in lines:
        if "交易--股票和股票期權" in line or "交易--股票和股票期权" in line:
            in_trades = True
            current_symbol = None
            current_name = None
            current_side = None
            continue
        if in_trades and "交易--基金" in line:
            in_trades = False
            current_symbol = None
            current_name = None
            current_side = None
            continue
        if not in_trades:
            continue

        header_match = re.search(r"(買入|賣出|賣出平倉)\s+([A-Z0-9.]+)\(([^)]*)\)", line)
        if header_match:
            current_side = "BUY" if header_match.group(1) == "買入" else "SELL"
            current_symbol = header_match.group(2)
            current_name = header_match.group(3).strip()
            continue
        header_partial = re.search(r"(買入|賣出|賣出平倉)\s+([A-Z0-9.]+)\(([^)]*)$", line)
        if header_partial:
            current_side = "BUY" if header_partial.group(1) == "買入" else "SELL"
            current_symbol = header_partial.group(2)
            current_name = header_partial.group(3).strip()
            continue

        row_match = re.search(
            r"(SEHK|US)\s+(HKD|USD|CNH|JPY|SGD)\s+"
            r"(\d{4}/\d{2}/\d{2})\s+(\d{4}/\d{2}/\d{2})\s+"
            r"([\d,]+)\s+([\d.]+)\s+([\d,]+(?:\.\d+)?)",
            line,
        )
        if row_match and current_symbol and current_side:
            trade_date = _parse_date(row_match.group(3), fmt="%Y/%m/%d")
            qty = _parse_number(row_match.group(5))
            price = _parse_number(row_match.group(6))
            currency = row_match.group(2)
            # Filter to stocks only (skip options)
            if current_symbol.endswith((".US", ".HK")) or re.search(r"\d{6,}[CP]\d{4,}", current_symbol):
                continue
            if trade_date and qty is not None and price is not None:
                trades.append(
                    Trade(
                        account_id=account_id,
                        symbol=current_symbol,
                        name=current_name,
                        currency=currency,
                        trade_date=trade_date,
                        side=current_side,
                        qty=abs(qty),
                        price=price,
                        source=f"交易:{current_symbol}",
                    )
                )

    holdings: List[Holding] = []
    section_lines = _extract_section_lines(
        text,
        "期初概覽--股票和股票期權",
        ["期初概覽--基金", "交易--股票和股票期權", "交易--股票和股票期权"],
    )
    if not section_lines:
        section_lines = _extract_section_lines(
            text,
            "期初概覽--股票和股票期权",
            ["期初概覽--基金", "交易--股票和股票期權", "交易--股票和股票期权"],
        )
    for line in section_lines:
        m = re.match(
            r"^([A-Z0-9.]+)\(([^)]*)\)\s+(SEHK|US)\s+(HKD|USD|CNH|JPY|SGD)\s+"
            r"([\d,]+)\s+([\d.]+)\s+-\s+([\d,]+(?:\.\d+)?)",
            line,
        )
        if not m:
            continue
        symbol = m.group(1)
        name = m.group(2)
        currency = m.group(4)
        qty = _parse_number(m.group(5))
        if qty is None:
            continue
        holdings.append(
            Holding(account_id=account_id, symbol=symbol, currency=currency, qty=float(qty), name=name)
        )

    return month, trades, holdings, account_id


def parse_pdf(pdf_path: str) -> Tuple[Optional[Tuple[int, int]], List[Trade], List[Holding], str]:
    pages = extract_text_pages(pdf_path)
    full_text = "\n".join(pages)

    if "保證金綜合帳戶" in full_text or "證券月結單" in full_text:
        return parse_futu(full_text)

    return parse_huatai(full_text)
