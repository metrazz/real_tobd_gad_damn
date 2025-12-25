import os
from datetime import date
from typing import Dict, List, Tuple

import dask.dataframe as dd
import pandas as pd
import requests
from prefect import flow, get_run_logger, task
from sqlalchemy import create_engine

# Base URL for the MOEX ISS API
MOEX_BASE = "https://iss.moex.com/iss"
# Default Postgres connection string (overridable via env)
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+psycopg2://prefect:prefect@postgres:5432/moex"
)
# Local threads will be used for Dask compute to avoid scheduler connectivity issues.
DASK_SCHEDULER = None


def _fetch_candles(ticker: str, trade_date: str) -> pd.DataFrame:
    """
    Fetch 1-minute candles for a ticker and date from MOEX ISS.
    """
    endpoint = (
        f"{MOEX_BASE}/engines/stock/markets/shares/boards/TQBR/"
        f"securities/{ticker}/candles.json"
    )
    params = {"from": trade_date, "till": trade_date, "interval": 1}
    resp = requests.get(endpoint, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    table = payload.get("candles", {})
    cols = table.get("columns", [])
    data = table.get("data", [])
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data, columns=cols)
    df["ticker"] = ticker
    # Rename to consistent names
    df = df.rename(
        columns={
            "begin": "ts_start",
            "end": "ts_end",
            "value": "turnover",
            "volume": "volume",
        }
    )
    return df


@task(retries=2, retry_delay_seconds=5)
def extract_candles(ticker: str, trade_date: str) -> pd.DataFrame:
    """Task: fetch candles for ticker/date."""
    logger = get_run_logger()
    df = _fetch_candles(ticker, trade_date)
    if df.empty:
        logger.warning(f"No candle data returned for {ticker} on {trade_date}")
    return df


def _build_candle_ddf(df: pd.DataFrame, npartitions: int = 2) -> dd.DataFrame:
    """Create Dask DF with datetime columns."""
    if df.empty:
        return dd.from_pandas(df, npartitions=1)
    work = df.copy()
    for col in ("ts_start", "ts_end"):
        if col in work.columns:
            work[col] = pd.to_datetime(work[col], errors="coerce")
    for col in ("open", "high", "low", "close", "turnover", "volume"):
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")
    return dd.from_pandas(work, npartitions=npartitions)


@task
def transform_candles(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Clean raw candles and add rolling volatility metric."""
    logger = get_run_logger()
    if df.empty:
        return df, pd.DataFrame()
    ddf = _build_candle_ddf(df)
    ddf = ddf.persist(scheduler="threads")
    ddf = ddf.dropna(subset=["ts_start", "close"])
    ddf = ddf.assign(ts_minute=ddf["ts_start"])
    # Rolling std per ticker on close price
    def _add_vol(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.sort_values("ts_minute")
        pdf["vol_std"] = (
            pdf.groupby("ticker")["close"]
            .rolling(15, min_periods=5)
            .std()
            .reset_index(level=0, drop=True)
        )
        return pdf

    ddf = ddf.map_partitions(_add_vol)
    raw_df = ddf.compute(scheduler="threads")
    # Aggregated table: already minute-level, keep required columns
    agg_df = raw_df[
        [
            "ts_minute",
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vol_std",
        ]
    ]
    return raw_df, agg_df


@task
def load_to_postgres(raw_df: pd.DataFrame, agg_df: pd.DataFrame) -> None:
    """Load raw candles and aggregated data into Postgres schemas raw and mart."""
    logger = get_run_logger()
    if raw_df.empty and agg_df.empty:
        logger.warning("Nothing to load into Postgres.")
        return
    engine = create_engine(DATABASE_URL)
    with engine.begin() as conn:
        if not raw_df.empty:
            raw_df.to_sql(
                "candles",
                conn,
                schema="raw",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )
        if not agg_df.empty:
            agg_df.to_sql(
                "agg_trades",
                conn,
                schema="mart",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=2000,
            )
    logger.info(
        f"Loaded raw={len(raw_df)} rows, agg={len(agg_df)} rows into Postgres."
    )


@flow(name="moex-etl")
def moex_etl(tickers: List[str], trade_date: str | None = None) -> None:
    """End-to-end ETL: extract candles, compute metrics, load into Postgres."""
    logger = get_run_logger()
    trade_date = trade_date or str(date.today())
    logger.info(f"Starting MOEX ETL for {trade_date} tickers={tickers}")
    extracted = [extract_candles.submit(ticker, trade_date) for ticker in tickers]
    transformed = [transform_candles.submit(e) for e in extracted]
    load_results = []
    for fut in transformed:
        raw_df, agg_df = fut.result()
        load_results.append(load_to_postgres.submit(raw_df, agg_df))
    for fut in load_results:
        fut.result()
    logger.info("MOEX ETL completed.")


if __name__ == "__main__":
    # Minimal local run example
    moex_etl(tickers=["SBER", "GAZP"], trade_date=str(date.today()))
