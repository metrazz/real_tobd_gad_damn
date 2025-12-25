CREATE DATABASE moex;
\connect moex;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS raw.candles (
    ts_start timestamp,
    ts_end timestamp,
    ticker text,
    open numeric,
    high numeric,
    low numeric,
    close numeric,
    turnover numeric,
    volume numeric,
    ts_minute timestamp,
    vol_std numeric
);

CREATE INDEX IF NOT EXISTS idx_candles_time_ticker ON raw.candles (ts_start, ticker);

CREATE TABLE IF NOT EXISTS raw.trades (
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

CREATE INDEX IF NOT EXISTS idx_trades_time_ticker ON raw.trades (trade_time, ticker);

CREATE TABLE IF NOT EXISTS mart.agg_trades (
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

CREATE INDEX IF NOT EXISTS idx_agg_trades_time_ticker ON mart.agg_trades (ts_minute, ticker);
