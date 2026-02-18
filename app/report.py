from dataclasses import dataclass
from typing import Dict, List, Optional

from openpyxl import Workbook


@dataclass
class SummaryRow:
    symbol: str
    currency: str
    gain: float
    loss: float
    net: float
    tax_base: float
    tax_due: float
    fx_rate: Optional[float]
    net_cny: Optional[float]
    tax_cny: Optional[float]
    cost_missing: bool


@dataclass
class WarningRow:
    symbol: str
    message: str


def build_workbook(
    rows: List[SummaryRow],
    warnings: List[WarningRow],
) -> Workbook:
    wb = Workbook()
    ws = wb.active
    ws.title = "Summary"
    ws.append(
        [
            "Symbol",
            "Currency",
            "Realized Gain",
            "Realized Loss",
            "Net (Gain-Loss)",
            "Tax Base",
            "Tax Due (20%)",
            "FX Rate (CNY)",
            "Net (CNY)",
            "Tax Due (CNY)",
            "Cost Missing",
        ]
    )
    for r in rows:
        ws.append(
            [
                r.symbol,
                r.currency,
                r.gain,
                r.loss,
                r.net,
                r.tax_base,
                r.tax_due,
                r.fx_rate,
                r.net_cny,
                r.tax_cny,
                "YES" if r.cost_missing else "",
            ]
        )

    ws2 = wb.create_sheet("Warnings")
    ws2.append(["Symbol", "Message"])
    for w in warnings:
        ws2.append([w.symbol, w.message])

    return wb
