import csv
import io
import os
import tempfile
from typing import Dict, List, Optional, Tuple
from uuid import uuid4

from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from .fifo import Lot, WarningMsg, compute_realized
from .parser import Holding, Trade, parse_pdf
from .report import SummaryRow, WarningRow, build_workbook

app = FastAPI()
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
REPORT_STORE: Dict[str, str] = {}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    defaults = {
        "usd_rate": 7.0288,
        "hkd_rate": 0.90322,
        "sgd_rate": 5.4586,
        "rate_date": "2025-12-31",
    }
    return templates.TemplateResponse("index.html", {"request": request, **defaults})


def _parse_fx_rates(usd_rate: str, hkd_rate: str, sgd_rate: str) -> Dict[str, float]:
    rates: Dict[str, float] = {}
    try:
        rates["USD"] = float(usd_rate)
    except (TypeError, ValueError):
        pass
    try:
        rates["HKD"] = float(hkd_rate)
    except (TypeError, ValueError):
        pass
    try:
        rates["SGD"] = float(sgd_rate)
    except (TypeError, ValueError):
        pass
    return rates


def _parse_avg_costs(file: Optional[UploadFile]) -> Dict[Tuple[str, str, str], float]:
    if file is None:
        return {}
    content = file.file.read()
    text = content.decode("utf-8", errors="ignore")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return {}
    start_idx = 0
    header = [c.strip().lower() for c in rows[0]]
    has_header = "symbol" in header and "currency" in header
    has_account = "account" in header or "account_id" in header
    if has_header:
        start_idx = 1
    costs: Dict[Tuple[str, str, str], float] = {}
    for row in rows[start_idx:]:
        if len(row) < 3:
            continue
        symbol = row[0].strip().upper()
        currency = row[1].strip().upper()
        try:
            avg_cost = float(row[2].strip())
        except ValueError:
            continue
        account_id = "*"
        if has_account and len(row) >= 4:
            account_id = row[3].strip()
        if not symbol or not currency:
            continue
        costs[(account_id, symbol, currency)] = avg_cost
    return costs


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace("*", "")


def _key(symbol: str, currency: str) -> Tuple[str, str]:
    return (_normalize_symbol(symbol), currency.upper())


def _cost_lookup(
    costs: Dict[Tuple[str, str, str], float],
    account_id: str,
    symbol: str,
    currency: str,
) -> Optional[float]:
    return costs.get((account_id, symbol, currency)) or costs.get(("*", symbol, currency))


@app.post("/process")
async def process(
    request: Request,
    statements: List[UploadFile] = File(...),
    avg_costs_csv: Optional[UploadFile] = File(None),
    usd_rate: str = Form(""),
    hkd_rate: str = Form(""),
    sgd_rate: str = Form(""),
    tax_floor_zero: Optional[str] = Form(None),
    target_year: Optional[str] = Form(None),
):
    if not statements:
        return templates.TemplateResponse(
            "index.html",
            {"request": request, "error": "请至少上传一个月结单 PDF。"},
            status_code=400,
        )

    tmp_dir = tempfile.mkdtemp(prefix="tax_app_")
    parsed: List[Tuple[Optional[Tuple[int, int]], List[Trade], List[Holding], str]] = []

    for f in statements:
        path = os.path.join(tmp_dir, f.filename)
        with open(path, "wb") as out:
            out.write(await f.read())
        parsed.append(parse_pdf(path))

    months = [p[0] for p in parsed if p[0] is not None]
    if not months:
        return templates.TemplateResponse(
            "index.html",
            {"request": request, "error": "无法识别月结单月份，请检查 PDF 格式。"},
            status_code=400,
        )

    years = sorted({y for y, _ in months})
    year: Optional[int] = None
    if target_year and target_year.strip().isdigit():
        year = int(target_year.strip())
    elif len(years) == 1:
        year = years[0]
    else:
        return templates.TemplateResponse(
            "index.html",
            {"request": request, "error": "检测到多年度月结单，请选择目标年份。"},
            status_code=400,
        )

    account_month_to_holdings: Dict[str, Dict[Tuple[int, int], List[Holding]]] = {}
    account_trades: Dict[str, List[Trade]] = {}

    for month, trades, holdings, account_id in parsed:
        if month:
            account_month_to_holdings.setdefault(account_id, {})[month] = holdings
        account_trades.setdefault(account_id, []).extend(trades)

    avg_costs = _parse_avg_costs(avg_costs_csv)
    rates = _parse_fx_rates(usd_rate, hkd_rate, sgd_rate)

    rows: List[SummaryRow] = []
    warnings: List[WarningRow] = []

    for account_id, trades in account_trades.items():
        month_map = account_month_to_holdings.get(account_id, {})
        earliest_month = sorted(month_map.keys())[0] if month_map else None
        initial_holdings = month_map.get(earliest_month, []) if earliest_month else []

        fallback_costs: Dict[str, float] = {}
        initial_lots: Dict[str, List[Lot]] = {}
        cost_missing_symbols = set()

        name_map: Dict[str, str] = {}
        for h in initial_holdings:
            if h.qty <= 0:
                continue
            sym = _normalize_symbol(h.symbol)
            if h.name:
                name_map[sym] = h.name
            key = _key(sym, h.currency)
            cost = _cost_lookup(avg_costs, account_id, sym, h.currency)
            if cost is not None:
                initial_lots.setdefault(sym, []).append(Lot(qty=h.qty, cost=cost))
                fallback_costs[sym] = cost
            else:
                warnings.append(
                    WarningRow(
                        account_id=account_id,
                        symbol=sym,
                        message=(
                            "Year-start holding detected but no average cost provided. "
                            "If this stock is sold before new buys, a 0 cost will be used."
                        ),
                    )
                )
                cost_missing_symbols.add(sym)

        realized, fifo_warnings, fifo_missing = compute_realized(
            trades, initial_lots, fallback_costs, target_year=year
        )
        cost_missing_symbols.update(fifo_missing)
        for w in fifo_warnings:
            warnings.append(WarningRow(account_id=account_id, symbol=w.symbol, message=w.message))

        warning_map: Dict[str, List[str]] = {}
        for w in warnings:
            if w.account_id != account_id:
                continue
            warning_map.setdefault(w.symbol, []).append(w.message)

        for sym, r in realized.items():
            cur = None
            for t in trades:
                if _normalize_symbol(t.symbol) == sym:
                    cur = t.currency
                    if not name_map.get(sym) and t.name:
                        name_map[sym] = t.name
                    break
            if cur is None:
                cur = ""
            symbol_name = name_map.get(sym, "")
            net = r.gain - r.loss
            tax_base = net
            tax_due = tax_base * 0.20
            tax_floor = str(tax_floor_zero).lower() in ("true", "on", "1", "yes")
            if tax_floor and tax_due < 0:
                tax_due = 0.0
            fx = rates.get(cur)
            if fx is None:
                warnings.append(
                    WarningRow(
                        account_id=account_id,
                        symbol=sym,
                        message=f"Missing FX rate for {cur}. CNY fields left blank.",
                    )
                )
            net_cny = net * fx if fx is not None else None
            tax_cny = tax_due * fx if fx is not None else None
            rows.append(
                SummaryRow(
                    account_id=account_id,
                    symbol=sym,
                    symbol_name=symbol_name,
                    currency=cur,
                    gain=r.gain,
                    loss=r.loss,
                    net=net,
                    tax_base=tax_base,
                    tax_due=tax_due,
                    fx_rate=fx,
                    net_cny=net_cny,
                    tax_cny=tax_cny,
                    cost_missing=sym in cost_missing_symbols,
                    cost_missing_reason="; ".join(warning_map.get(sym, [])) or None,
                )
            )

    totals = {
        "gain": sum(r.gain for r in rows),
        "loss": sum(r.loss for r in rows),
        "net": sum(r.net for r in rows),
        "tax_due": sum(r.tax_due for r in rows),
        "net_cny": sum(r.net_cny or 0 for r in rows),
        "tax_cny": sum(r.tax_cny or 0 for r in rows),
    }
    rows.append(
        SummaryRow(
            account_id="TOTAL",
            symbol="",
            symbol_name="汇总",
            currency="",
            gain=totals["gain"],
            loss=totals["loss"],
            net=totals["net"],
            tax_base=totals["net"],
            tax_due=totals["tax_due"],
            fx_rate=None,
            net_cny=totals["net_cny"],
            tax_cny=totals["tax_cny"],
            cost_missing=False,
            cost_missing_reason=None,
        )
    )

    wb = build_workbook(rows, warnings)
    out_path = os.path.join(tmp_dir, f"tax_report_{year}.xlsx")
    wb.save(out_path)

    token = uuid4().hex
    REPORT_STORE[token] = out_path

    accounts = sorted({r.account_id for r in rows if r.account_id != "TOTAL"})
    return templates.TemplateResponse(
        "preview.html",
        {
            "request": request,
            "rows": rows,
            "warnings": warnings,
            "download_url": f"/download/{token}",
            "year": year,
            "accounts": accounts,
            "totals": totals,
        },
    )


@app.get("/download/{token}")
def download(token: str):
    path = REPORT_STORE.get(token)
    if not path or not os.path.exists(path):
        return HTMLResponse("文件不存在或已过期。", status_code=404)
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=os.path.basename(path),
    )
