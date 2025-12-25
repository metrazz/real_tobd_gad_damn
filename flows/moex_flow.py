import os  # работа с переменными окружения
from datetime import date  # работа с датами
from typing import Dict, List, Tuple  # типы для аннотаций

import dask.dataframe as dd  # Dask DataFrame API
import pandas as pd  # базовая работа с таблицами
import requests  # HTTP-запросы к ISS API
from prefect import flow, get_run_logger, task  # Prefect flow и задачи
from sqlalchemy import create_engine  # подключение к Postgres

MOEX_BASE = "https://iss.moex.com/iss"  # базовый URL ISS API
DATABASE_URL = os.getenv(  # строка подключения к Postgres из окружения
    "DATABASE_URL", "postgresql+psycopg2://prefect:prefect@postgres:5432/moex"
)
DASK_SCHEDULER = None  # используем локальные threads для Dask


def _fetch_candles(ticker: str, trade_date: str) -> pd.DataFrame:
    """Загрузка минутных свечей для тикера и даты."""
    endpoint = (  # формируем путь до эндпоинта свечей
        f"{MOEX_BASE}/engines/stock/markets/shares/boards/TQBR/"
        f"securities/{ticker}/candles.json"
    )
    params = {"from": trade_date, "till": trade_date, "interval": 1}  # параметры запроса
    resp = requests.get(endpoint, params=params, timeout=30)  # HTTP GET с таймаутом
    resp.raise_for_status()  # выброс исключения при ошибке HTTP
    payload = resp.json()  # разбираем JSON-ответ
    table = payload.get("candles", {})  # извлекаем секцию свечей
    cols = table.get("columns", [])  # список колонок
    data = table.get("data", [])  # массив данных
    if not data:  # если пусто, возвращаем пустой DataFrame
        return pd.DataFrame()
    df = pd.DataFrame(data, columns=cols)  # строим DataFrame из данных
    df["ticker"] = ticker  # добавляем колонку тикера
    df = df.rename(  # переименовываем колонки к единым именам
        columns={
            "begin": "ts_start",
            "end": "ts_end",
            "value": "turnover",
            "volume": "volume",
        }
    )
    return df  # отдаём подготовленный DataFrame


@task(retries=2, retry_delay_seconds=5)
def extract_candles(ticker: str, trade_date: str) -> pd.DataFrame:
    """Prefect-задача: загрузить свечи для тикера/даты."""
    logger = get_run_logger()  # логгер Prefect
    df = _fetch_candles(ticker, trade_date)  # вызываем загрузку
    if df.empty:  # если данных нет, предупреждаем
        logger.warning(f"No candle data returned for {ticker} on {trade_date}")
    return df  # возвращаем DataFrame свечей


def _build_candle_ddf(df: pd.DataFrame, npartitions: int = 2) -> dd.DataFrame:
    """Перевод pandas DataFrame свечей в Dask DF с нормализацией типов."""
    if df.empty:  # если пусто, создаём минимальный Dask DF
        return dd.from_pandas(df, npartitions=1)
    work = df.copy()  # копируем, чтобы не трогать исходные данные
    for col in ("ts_start", "ts_end"):  # конвертируем временные колонки
        if col in work.columns:
            work[col] = pd.to_datetime(work[col], errors="coerce")
    for col in ("open", "high", "low", "close", "turnover", "volume"):  # числовые поля
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")
    return dd.from_pandas(work, npartitions=npartitions)  # создаём Dask DF


@task
def transform_candles(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Очистка свечей и расчёт волатильности."""
    logger = get_run_logger()  # логгер Prefect
    if df.empty:  # если данных нет, возвращаем пустые структуры
        return df, pd.DataFrame()
    ddf = _build_candle_ddf(df)  # готовим Dask DF
    ddf = ddf.persist(scheduler="threads")  # материализуем вычисления на threads
    ddf = ddf.dropna(subset=["ts_start", "close"])  # удаляем строки без времени/цены
    ddf = ddf.assign(ts_minute=ddf["ts_start"])  # добавляем минутный таймстамп

    def _add_vol(pdf: pd.DataFrame) -> pd.DataFrame:
        """Расчёт rolling std по close внутри тикера."""
        pdf = pdf.sort_values("ts_minute")  # упорядочиваем по времени
        pdf["vol_std"] = (
            pdf.groupby("ticker")["close"]
            .rolling(15, min_periods=5)
            .std()
            .reset_index(level=0, drop=True)
        )
        return pdf  # возвращаем обогащённый фрейм

    ddf = ddf.map_partitions(_add_vol)  # считаем волатильность по партициям
    raw_df = ddf.compute(scheduler="threads")  # собираем результат в pandas
    agg_df = raw_df[  # формируем витрину с нужными колонками
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
    return raw_df, agg_df  # отдаём сырые и агрегированные данные


@task
def load_to_postgres(raw_df: pd.DataFrame, agg_df: pd.DataFrame) -> None:
    """Загрузка сырых и агрегированных данных в Postgres."""
    logger = get_run_logger()  # логгер Prefect
    if raw_df.empty and agg_df.empty:  # если нечего грузить — выходим
        logger.warning("Nothing to load into Postgres.")
        return
    engine = create_engine(DATABASE_URL)  # создаём engine SQLAlchemy
    with engine.begin() as conn:  # открываем транзакцию
        if not raw_df.empty:  # грузим сырые свечи
            raw_df.to_sql(
                "candles",
                conn,
                schema="raw",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=5000,
            )
        if not agg_df.empty:  # грузим витрину
            agg_df.to_sql(
                "agg_trades",
                conn,
                schema="mart",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=2000,
            )
    logger.info(  # логируем объём загрузки
        f"Loaded raw={len(raw_df)} rows, agg={len(agg_df)} rows into Postgres."
    )


@flow(name="moex-etl")
def moex_etl(tickers: List[str], trade_date: str | None = None) -> None:
    """Полный ETL: загрузка свечей, расчёт метрик, запись в Postgres."""
    logger = get_run_logger()  # логгер Prefect
    trade_date = trade_date or str(date.today())  # по умолчанию сегодня
    logger.info(f"Starting MOEX ETL for {trade_date} tickers={tickers}")  # стартовый лог
    extracted = [extract_candles.submit(ticker, trade_date) for ticker in tickers]  # параллельный extract
    transformed = [transform_candles.submit(e) for e in extracted]  # параллельный transform
    load_results = []  # буфер futures загрузки
    for fut in transformed:  # перебираем результаты трансформаций
        raw_df, agg_df = fut.result()  # получаем pandas DataFrame
        load_results.append(load_to_postgres.submit(raw_df, agg_df))  # создаём загрузку
    for fut in load_results:  # ждём завершения загрузок
        fut.result()
    logger.info("MOEX ETL completed.")  # финальный лог


if __name__ == "__main__":  # точка входа для локального запуска
    moex_etl(tickers=["SBER", "GAZP"], trade_date=str(date.today()))  # пример запуска
