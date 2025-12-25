"""
Reusable Dask-based transformations for MOEX trades.
"""

import dask.dataframe as dd
import pandas as pd


def build_trades_ddf(df: pd.DataFrame, npartitions: int = 4) -> dd.DataFrame:
    """Convert raw trades pandas DataFrame into a Dask DataFrame with clean types."""
    work = df.copy()
    if "trade_time" in work.columns:
        work["trade_time"] = pd.to_datetime(work["trade_time"], errors="coerce")
    for col in ("price", "quantity", "value"):
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["trade_time", "price", "quantity"])
    return dd.from_pandas(work, npartitions=npartitions)


def compute_minute_agg(ddf: dd.DataFrame) -> dd.DataFrame:
    """Aggregate trades into OHLCV per minute and add rolling volatility."""
    ddf["ts_minute"] = ddf["trade_time"].dt.floor("min")
    grouped = ddf.groupby(["ticker", "ts_minute"])
    agg = grouped.agg(
        {
            "price": ["first", "max", "min", "last"],
            "quantity": "sum",
            "trade_id": "count",
        }
    )
    agg.columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trades_count",
    ]
    agg = agg.reset_index()
    agg = agg.set_index("ts_minute")
    agg = agg.map_partitions(
        lambda pdf: pdf.assign(
            vol_std=pdf.groupby("ticker")["close"].rolling(15, min_periods=5).std().values
        )
    ).reset_index()
    return agg
