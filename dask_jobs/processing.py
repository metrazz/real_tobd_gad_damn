"""Переиспользуемые преобразования Dask для трейдов MOEX."""  # описание модуля

import dask.dataframe as dd  # Dask DataFrame API
import pandas as pd  # pandas для работы с таблицами


def build_trades_ddf(df: pd.DataFrame, npartitions: int = 4) -> dd.DataFrame:
    """Конвертация pandas-трейдов в Dask DF с нормализацией типов."""
    work = df.copy()  # копия данных, чтобы не трогать исходные
    if "trade_time" in work.columns:  # если есть время сделки
        work["trade_time"] = pd.to_datetime(work["trade_time"], errors="coerce")  # в datetime
    for col in ("price", "quantity", "value"):  # числовые поля
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")  # в числа
    work = work.dropna(subset=["trade_time", "price", "quantity"])  # убираем пустые ключевые поля
    return dd.from_pandas(work, npartitions=npartitions)  # создаём Dask DF


def compute_minute_agg(ddf: dd.DataFrame) -> dd.DataFrame:
    """Агрегация трейдов в минутные OHLCV и расчёт волатильности."""
    ddf["ts_minute"] = ddf["trade_time"].dt.floor("min")  # минутный таймстамп
    grouped = ddf.groupby(["ticker", "ts_minute"])  # группируем по тикеру и минуте
    agg = grouped.agg(  # считаем OHLCV и число сделок
        {
            "price": ["first", "max", "min", "last"],
            "quantity": "sum",
            "trade_id": "count",
        }
    )
    agg.columns = [  # распаковываем многоуровневые имена колонок
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trades_count",
    ]
    agg = agg.reset_index()  # возвращаем индексы в колонки
    agg = agg.set_index("ts_minute")  # ставим время в индекс
    agg = agg.map_partitions(  # считаем rolling std по close
        lambda pdf: pdf.assign(
            vol_std=pdf.groupby("ticker")["close"].rolling(15, min_periods=5).std().values
        )
    ).reset_index()
    return agg  # возвращаем агрегированный Dask DF
