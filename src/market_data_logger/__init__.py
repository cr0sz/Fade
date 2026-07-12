"""Passive Binance funding-rate & liquidation logger.

Accumulates the panic-moment dataset (liquidation cascades + funding rates)
that this system's fade edge trades, for a future signal engine. Reads no API
keys — public USDT-perp endpoints only. Writes to funding_rates and
liquidation_events (migration 023). Nothing trades off this yet.
"""
