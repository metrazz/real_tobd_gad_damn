# MOEX ETL: Prefect + Dask (threads) + PostgreSQL + Streamlit

Готовый каркас для загрузки минутных свечей Московской биржи из ISS API, расчёта метрик и визуализации.

## Состав
- **Prefect 2.x** — оркестрация ETL.
- **Dask (threads)** — параллельные вычисления без внешнего scheduler (устойчиво к обрывам).
- **PostgreSQL** — схемы `raw` (сырые свечи) и `mart` (агрегация).
- **Streamlit** — дашборд по витрине.
- **Docker Compose** — разворачивание всего окружения.

## Быстрый старт (Docker)
```bash
# 1) Поднять Postgres
docker compose up -d postgres
# 2) Остальные сервисы
docker compose up -d scheduler worker prefect agent streamlit
# 3) Проверить
docker compose ps
```
UI:
- Prefect: http://localhost:4200
- Dask: http://localhost:8787
- Streamlit: http://localhost:8501

Инициализация БД (создаёт `raw.candles`, `raw.trades`, `mart.agg_trades`):
```bash
docker compose exec postgres psql -U prefect -d moex -f /docker-entrypoint-initdb.d/init.sql
```

## Деплой Prefect
В контейнере `agent`:
```bash
docker compose exec agent bash -lc "
  pip install -r requirements.txt &&
  prefect config set PREFECT_API_URL=http://prefect:4200/api &&
  prefect deployment build flows/moex_flow.py:moex_etl -n moex &&
  prefect deployment apply moex_etl-deployment.yaml &&
  prefect work-queue create default || true
"
```

## Запуск ETL
Пример для торговых дней (минутные свечи):
```bash
docker compose exec agent bash -lc \
  "prefect deployment run moex-etl/moex --params '{\"tickers\":[\"SBER\",\"GAZP\"],\"trade_date\":\"2024-06-03\"}'"

docker compose exec agent bash -lc \
  "prefect deployment run moex-etl/moex --params '{\"tickers\":[\"SBER\",\"GAZP\"],\"trade_date\":\"2024-06-04\"}'"
```
Важно: MOEX ISS не отдаёт данные за выходные/праздники — выбирайте торговые дни.

## Проверка загрузки
```bash
docker compose exec postgres psql -U prefect -d moex -c "select count(*) from raw.candles;"
docker compose exec postgres psql -U prefect -d moex -c "select count(*) from mart.agg_trades;"
docker compose exec postgres psql -U prefect -d moex -c "select distinct ticker from mart.agg_trades;"
docker compose exec postgres psql -U prefect -d moex -c "select ts_minute, ticker, close, volume from mart.agg_trades order by ts_minute desc limit 5;"
```

## Дашборд
- Открыть http://localhost:8501
- Выбрать тикеры/дату, по которым есть данные.

## Структура
- `flows/moex_flow.py` — Prefect Flow: `extract_candles`, `transform_candles`, `load_to_postgres`.
- `dask_jobs/processing.py` — вспомогательные функции Dask.
- `dashboards/app.py` — Streamlit-дэшборд.
- `db/init.sql` — схемы и таблицы (`raw.candles`, `raw.trades`, `mart.agg_trades`).
- `docker-compose.yml` — сервисы Postgres, Prefect, Dask, Streamlit.
- `quickstart.md` — расширенный пошаговый гайд.

## Пайплайн (что делает)
1) `extract_candles` — тянет минутные свечи из ISS API по списку тикеров и дате.
2) `transform_candles` — чистит данные, считает rolling std по close, формирует витрину.
3) `load_to_postgres` — пишет сырые свечи в `raw.candles`, витрину в `mart.agg_trades`.

Состав полей витрины `mart.agg_trades`: `ts_minute`, `ticker`, `open`, `high`, `low`, `close`, `volume`, `vol_std`.

## Локальный запуск без Docker (опционально)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=postgresql+psycopg2://prefect:prefect@localhost:5432/moex
python flows/moex_flow.py
```

## Отладка и логи
- Prefect логи: `docker compose logs -f agent`
- Worker логи (Dask threads всё равно локально): `docker compose logs -f worker`
- Dask dashboard: http://localhost:8787
- Prefect UI: http://localhost:4200
