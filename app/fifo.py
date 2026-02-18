from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

from .parser import Trade


@dataclass
class Lot:
    qty: float
    cost: float  # cost per share


@dataclass
class Realized:
    gain: float = 0.0
    loss: float = 0.0


@dataclass
class WarningMsg:
    symbol: str
    message: str


def _add_realized(realized: Realized, amount: float) -> None:
    if amount >= 0:
        realized.gain += amount
    else:
        realized.loss += -amount


def compute_realized(
    trades: List[Trade],
    initial_lots: Dict[str, List[Lot]],
    fallback_costs: Dict[str, float],
    target_year: int | None = None,
) -> Tuple[Dict[str, Realized], List[WarningMsg], Set[str]]:
    positions: Dict[str, List[Lot]] = {sym: list(lots) for sym, lots in initial_lots.items()}
    realized: Dict[str, Realized] = {}
    warnings: List[WarningMsg] = []
    missing_cost_symbols: Set[str] = set()

    trades_sorted = sorted(trades, key=lambda t: t.trade_date)
    for trade in trades_sorted:
        sym = trade.symbol
        if sym not in positions:
            positions[sym] = []
        if sym not in realized:
            realized[sym] = Realized()

        if trade.side == "BUY":
            positions[sym].append(Lot(qty=trade.qty, cost=trade.price))
            continue

        remaining = trade.qty
        lots = positions[sym]
        while remaining > 1e-9 and lots:
            lot = lots[0]
            take = min(remaining, lot.qty)
            if target_year is None or trade.trade_date.year == target_year:
                _add_realized(realized[sym], (trade.price - lot.cost) * take)
            lot.qty -= take
            remaining -= take
            if lot.qty <= 1e-9:
                lots.pop(0)

        if remaining > 1e-9:
            fallback = fallback_costs.get(sym)
            if fallback is None:
                warnings.append(
                    WarningMsg(
                        symbol=sym,
                        message=(
                            "Sell quantity exceeds available lots and no year-start average cost provided. "
                            "Used 0 cost for remaining shares."
                        ),
                    )
                )
                missing_cost_symbols.add(sym)
                fallback = 0.0
            if target_year is None or trade.trade_date.year == target_year:
                _add_realized(realized[sym], (trade.price - fallback) * remaining)
            remaining = 0.0

    return realized, warnings, missing_cost_symbols
