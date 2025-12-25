# Quickstart: MOEX ETL (Prefect + Dask threads + PostgreSQL + Streamlit)

## Предпосылки
- Docker + Docker Compose установлен на хосте.
- Порт 5432/4200/8501/8787 свободны.

## 1) Поднять стек
```bash
docker compose up -d postgres
# дождитесь готовности Postgres (healthcheck в логах)
docker compose up -d scheduler worker prefect agent streamlit
docker compose ps
```
UI:
- Prefect: http://localhost:4200
- Dask: http://localhost:8787
- Streamlit: http://localhost:8501

## 2) Применить схему БД (создаёт raw.candles, raw.trades, mart.agg_trades)
```bash
docker compose exec postgres psql -U prefect -d moex -f /docker-entrypoint-initdb.d/init.sql
```

## 3) Собрать и применить Prefect deployment
```bash
docker compose exec agent bash -lc "
  pip install -r requirements.txt &&
  prefect config set PREFECT_API_URL=http://prefect:4200/api &&
  prefect deployment build flows/moex_flow.py:moex_etl -n moex &&
  prefect deployment apply moex_etl-deployment.yaml
"
```

## 4) Запустить flow (пример, торговые дни)
```bash
# СБЕР, ГАЗП за 3 и 4 июня 2024
docker compose exec agent bash -lc \
  "prefect deployment run moex-etl/moex --params '{\"tickers\":[\"SBER\",\"GAZP\"],\"trade_date\":\"2024-06-03\"}'"
docker compose exec agent bash -lc \
  "prefect deployment run moex-etl/moex --params '{\"tickers\":[\"SBER\",\"GAZP\"],\"trade_date\":\"2024-06-04\"}'"

# LKOH, YNDX за те же даты
docker compose exec agent bash -lc \
  "prefect deployment run moex-etl/moex --params '{\"tickers\":[\"LKOH\",\"YNDX\"],\"trade_date\":\"2024-06-03\"}'"
docker compose exec agent bash -lc \
  "prefect deployment run moex-etl/moex --params '{\"tickers\":[\"LKOH\",\"YNDX\"],\"trade_date\":\"2024-06-04\"}'"
```
- Торговые дни обязательны: за выходные MOEX ISS данных не отдаёт.

## 5) Проверить загрузку в БД
```bash
docker compose exec postgres psql -U prefect -d moex -c "select count(*) from raw.candles;"
docker compose exec postgres psql -U prefect -d moex -c "select count(*) from mart.agg_trades;"
docker compose exec postgres psql -U prefect -d moex -c "select distinct ticker from mart.agg_trades;"
docker compose exec postgres psql -U prefect -d moex -c "select ts_minute, ticker, close, volume from mart.agg_trades order by ts_minute desc limit 5;"
```

## 6) Посмотреть дашборд
- Открыть http://localhost:8501
- Выбрать тикеры и даты, по которым есть данные.

## 7) Мониторинг и логирование
```bash
docker compose logs -f agent     # Prefect агент и задачи
docker compose logs -f worker    # Dask worker (при больших нагрузках)
```
- Dask dashboard: http://localhost:8787
- Prefect UI: http://localhost:4200 (запуски, статусы).

## 8) Частые вопросы
- Нет данных: выбрана неторговая дата, или тикер отсутствует на TQBR.
- Соединение с Dask: по умолчанию вычисления на локальных threads (переменная `DASK_SCHEDULER` не нужна).
- Порты заняты: поменяйте mapping в `docker-compose.yml`.
