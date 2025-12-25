import os
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql+psycopg2://prefect:prefect@postgres:5432/moex"
)


@st.cache_data(ttl=300)
def load_tickers() -> list[str]:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        res = conn.execute(
            text(
                "select distinct ticker from mart.agg_trades order by ticker limit 200"
            )
        )
        return [r[0] for r in res.fetchall()]


@st.cache_data(ttl=120)
def load_agg(tickers: list[str], start_dt: date, end_dt: date) -> pd.DataFrame:
    engine = create_engine(DATABASE_URL)
    query = text(
        """
        select ts_minute, ticker, open, high, low, close, volume, trades_count, vol_std
        from mart.agg_trades
        where ticker = any(:tickers)
          and ts_minute >= :start_dt
          and ts_minute < :end_dt
        order by ts_minute
        """
    )
    params = {
        "tickers": tickers,
        "start_dt": start_dt,
        "end_dt": end_dt + timedelta(days=1),
    }
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params, parse_dates=["ts_minute"])
    return df


def main() -> None:
    st.set_page_config(page_title="MOEX Analytics", layout="wide")
    st.title("MOEX Trading Analytics")
    st.caption("Aggregations produced by Prefect + Dask ETL")

    tickers = load_tickers()
    default_date = date.today() - timedelta(days=1)
    col1, col2, col3 = st.columns(3)
    with col1:
        selected = st.multiselect("Tickers", tickers, default=tickers[:3])
    with col2:
        start_dt = st.date_input("From", default_date)
    with col3:
        end_dt = st.date_input("To", default_date)

    if not selected:
        st.info("Select at least one ticker to view data.")
        return

    df = load_agg(selected, start_dt, end_dt)
    if df.empty:
        st.warning("No aggregated data for the chosen window.")
        return

    st.subheader("Price (close) per minute")
    price_chart = (
        df.pivot_table(index="ts_minute", columns="ticker", values="close")
        .sort_index()
    )
    st.line_chart(price_chart)

    st.subheader("Volume per minute")
    volume_chart = (
        df.pivot_table(index="ts_minute", columns="ticker", values="volume")
        .sort_index()
    )
    st.area_chart(volume_chart)

    st.subheader("Volatility (rolling std)")
    vol_chart = (
        df.pivot_table(index="ts_minute", columns="ticker", values="vol_std")
        .sort_index()
    )
    st.line_chart(vol_chart)

    st.subheader("Top volume ticks")
    top_rows = (
        df.sort_values(["volume"], ascending=False)
        .head(50)
        .loc[:, ["ts_minute", "ticker", "open", "high", "low", "close", "volume"]]
    )
    st.dataframe(top_rows, height=400)


if __name__ == "__main__":
    main()
