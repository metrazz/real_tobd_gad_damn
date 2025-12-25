import os  # работа с переменными окружения
from datetime import date, timedelta  # даты и интервалы

import pandas as pd  # таблицы
import streamlit as st  # UI
from sqlalchemy import create_engine, text  # работа с Postgres

DATABASE_URL = os.getenv(  # строка подключения к БД
    "DATABASE_URL", "postgresql+psycopg2://prefect:prefect@postgres:5432/moex"
)


@st.cache_data(ttl=300)
def load_tickers() -> list[str]:
    """Загрузка списка доступных тикеров из витрины."""
    engine = create_engine(DATABASE_URL)  # создаём engine
    with engine.connect() as conn:  # открываем соединение
        res = conn.execute(  # выполняем запрос distinct тикеров
            text(
                "select distinct ticker from mart.agg_trades order by ticker limit 200"
            )
        )
        return [r[0] for r in res.fetchall()]  # превращаем результат в список


@st.cache_data(ttl=120)
def load_agg(tickers: list[str], start_dt: date, end_dt: date) -> pd.DataFrame:
    """Загрузка агрегатов для выбранных тикеров и дат."""
    engine = create_engine(DATABASE_URL)  # создаём engine
    query = text(  # готовим параметризованный запрос
        """
        select ts_minute, ticker, open, high, low, close, volume, trades_count, vol_std
        from mart.agg_trades
        where ticker = any(:tickers)
          and ts_minute >= :start_dt
          and ts_minute < :end_dt
        order by ts_minute
        """
    )
    params = {  # параметры запроса
        "tickers": tickers,
        "start_dt": start_dt,
        "end_dt": end_dt + timedelta(days=1),
    }
    with engine.connect() as conn:  # открываем соединение
        df = pd.read_sql(query, conn, params=params, parse_dates=["ts_minute"])  # читаем в pandas
    return df  # отдаём DataFrame


def main() -> None:
    """Главная функция приложения Streamlit."""
    st.set_page_config(page_title="MOEX Analytics", layout="wide")  # базовая конфигурация страницы
    st.title("MOEX Trading Analytics")  # заголовок
    st.caption("Aggregations produced by Prefect + Dask ETL")  # подпись

    tickers = load_tickers()  # получаем список тикеров
    default_date = date.today() - timedelta(days=1)  # дата по умолчанию
    col1, col2, col3 = st.columns(3)  # три колонки для контролов
    with col1:
        selected = st.multiselect("Tickers", tickers, default=tickers[:3])  # выбор тикеров
    with col2:
        start_dt = st.date_input("From", default_date)  # стартовая дата
    with col3:
        end_dt = st.date_input("To", default_date)  # конечная дата

    if not selected:  # если тикеры не выбраны — выводим подсказку
        st.info("Select at least one ticker to view data.")
        return

    df = load_agg(selected, start_dt, end_dt)  # грузим агрегаты
    if df.empty:  # если нет данных — предупреждаем
        st.warning("No aggregated data for the chosen window.")
        return

    st.subheader("Price (close) per minute")  # график цены
    price_chart = (
        df.pivot_table(index="ts_minute", columns="ticker", values="close")
        .sort_index()
    )
    st.line_chart(price_chart)  # линия цен

    st.subheader("Volume per minute")  # график объёма
    volume_chart = (
        df.pivot_table(index="ts_minute", columns="ticker", values="volume")
        .sort_index()
    )
    st.area_chart(volume_chart)  # area chart объёмов

    st.subheader("Volatility (rolling std)")  # график волатильности
    vol_chart = (
        df.pivot_table(index="ts_minute", columns="ticker", values="vol_std")
        .sort_index()
    )
    st.line_chart(vol_chart)  # линия волатильности

    st.subheader("Top volume ticks")  # таблица топ-объёмов
    top_rows = (
        df.sort_values(["volume"], ascending=False)
        .head(50)
        .loc[:, ["ts_minute", "ticker", "open", "high", "low", "close", "volume"]]
    )
    st.dataframe(top_rows, height=400)  # вывод таблицы


if __name__ == "__main__":  # точка входа для локального запуска
    main()  # запуск приложения
