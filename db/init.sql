CREATE DATABASE moex;  -- создаём базу данных moex
\connect moex;         -- переключаемся в новую базу

CREATE SCHEMA IF NOT EXISTS raw;   -- схема для сырых данных
CREATE SCHEMA IF NOT EXISTS mart;  -- схема для витрин/агрегаций

CREATE TABLE IF NOT EXISTS raw.candles (  -- сырые минутные свечи
    ts_start timestamp,   -- время начала интервала
    ts_end timestamp,     -- время конца интервала
    ticker text,          -- тикер инструмента
    open numeric,         -- цена открытия
    high numeric,         -- максимум
    low numeric,          -- минимум
    close numeric,        -- цена закрытия
    turnover numeric,     -- оборот в деньгах
    volume numeric,       -- объём в штуках
    ts_minute timestamp,  -- минутная метка (дублирует ts_start)
    vol_std numeric       -- скользящее стандартное отклонение
);

CREATE INDEX IF NOT EXISTS idx_candles_time_ticker ON raw.candles (ts_start, ticker);  -- индекс по времени и тикеру

CREATE TABLE IF NOT EXISTS raw.trades (  -- задел под сырые трейды
    trade_id bigint,
    trade_time timestamp,
    ticker text,
    price numeric,
    quantity numeric,
    value numeric,
    board text,
    buyer text,
    seller text
);

CREATE INDEX IF NOT EXISTS idx_trades_time_ticker ON raw.trades (trade_time, ticker);  -- индекс трейдов

CREATE TABLE IF NOT EXISTS mart.agg_trades (  -- витрина минутных OHLCV и волатильности
    ts_minute timestamp not null,
    ticker text not null,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    volume numeric,
    trades_count integer,
    vol_std numeric
);

CREATE INDEX IF NOT EXISTS idx_agg_trades_time_ticker ON mart.agg_trades (ts_minute, ticker);  -- индекс витрины
