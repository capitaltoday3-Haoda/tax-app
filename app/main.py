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
    return templates.TemplateResponse("index.html", {"request": request})


def _parse_fx_rates(raw: str) -> Dict[str, float]:
    rates: Dict[str, float] = {}
    if not raw:
        return rates
    parts = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        parts.extend([p.strip() for p in line.split(",") if p.strip()])
    for part in parts:
        if "=" not in part:
            continue
        cur, val = part.split("=", 1)
        cur = cur.strip().upper()
        try:
            rates[cur] = float(val.strip())
        except ValueError:
            continue
    return rates


def _parse_avg_costs(file: Optional[UploadFile]) -> Dict[Tuple[str, str], float]:
    if file is None:
        return {}
    content = file.file.read()
    text = content.decode("utf-8", errors="ignore")
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return {}
    # Detect header
    start_idx = 0
    header = [c.strip().lower() for c in rows[0]]
    if "symbol" in header and "currency" in header:
        start_idx = 1
    costs: Dict[Tuple[str, str], float] = {}
    for row in rows[start_idx:]:
        if len(row) < 3:
            continue
        symbol = row[0].strip().upper()
        currency = row[1].strip().upper()
        try:
            avg_cost = float(row[2].strip())
        except ValueError:
            continue
        if not symbol or not currency:
            continue
        costs[(symbol, currency)] = avg_cost
    return costs


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace("*", "")


def _key(symbol: str, currency: str) -> Tuple[str, str]:
    return (_normalize_symbol(symbol), currency.upper())


@app.post("/process")
async def process(
    request: Request,
    statements: List[UploadFile] = File(...),
    avg_costs_csv: Optional[UploadFile] = File(None),
    fx_rates: str = Form(""),
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
    parsed: List[Tuple[Optional[Tuple[int, int]], List[Trade], List[Holding]]] = []

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

    # Identify earliest month for initial holdings
    month_to_holdings: Dict[Tuple[int, int], List[Holding]] = {}
    all_trades: List[Trade] = []
    for month, trades, holdings in parsed:
        if month:
            month_to_holdings[month] = holdings
        all_trades.extend(trades)

    earliest_month = sorted(month_to_holdings.keys())[0]
    initial_holdings = month_to_holdings.get(earliest_month, [])

    avg_costs = _parse_avg_costs(avg_costs_csv)
    fallback_costs: Dict[str, float] = {}
    initial_lots: Dict[str, List[Lot]] = {}
    warnings: List[WarningRow] = []

    for h in initial_holdings:
        if h.qty <= 0:
            continue
        sym = _normalize_symbol(h.symbol)
        key = _key(sym, h.currency)
        if key in avg_costs:
            cost = avg_costs[key]
            initial_lots.setdefault(sym, []).append(Lot(qty=h.qty, cost=cost))
            fallback_costs[sym] = cost
        else:
            warnings.append(
                WarningRow(
                    symbol=sym,
                    message=(
                        "Year-start holding detected but no average cost provided. "
                        "If this stock is sold before new buys, a 0 cost will be used."
                    ),
                )
            )

    realized, fifo_warnings = compute_realized(all_trades, initial_lots, fallback_costs, target_year=year)
    for w in fifo_warnings:
        warnings.append(WarningRow(symbol=w.symbol, message=w.message))

    rates = _parse_fx_rates(fx_rates)

    rows: List[SummaryRow] = []
    for sym, r in realized.items():
        # Determine currency from trades
        cur = None
        for t in all_trades:
            if _normalize_symbol(t.symbol) == sym:
                cur = t.currency
                break
        if cur is None:
            cur = ""
        net = r.gain - r.loss
        tax_base = net
        tax_due = tax_base * 0.20
        tax_floor = str(tax_floor_zero).lower() in ("true", "on", "1", "yes")
        if tax_floor and tax_due < 0:
            tax_due = 0.0
        fx = rates.get(cur)
        if fx is None:
            warnings.append(
                WarningRow(symbol=sym, message=f"Missing FX rate for {cur}. CNY fields left blank.")
            )
        net_cny = net * fx if fx is not None else None
        tax_cny = tax_due * fx if fx is not None else None
        rows.append(
            SummaryRow(
                symbol=sym,
                currency=cur,
                gain=r.gain,
                loss=r.loss,
                net=net,
                tax_base=tax_base,
                tax_due=tax_due,
                fx_rate=fx,
                net_cny=net_cny,
                tax_cny=tax_cny,
            )
        )

    wb = build_workbook(rows, warnings)
    out_path = os.path.join(tmp_dir, f"tax_report_{year}.xlsx")
    wb.save(out_path)

    token = uuid4().hex
    REPORT_STORE[token] = out_path

    return templates.TemplateResponse(
        "preview.html",
        {
            "request": request,
            "rows": rows,
            "warnings": warnings,
            "download_url": f"/download/{token}",
            "year": year,
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
