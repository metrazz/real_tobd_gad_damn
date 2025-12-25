# MOEX Big Data ETL (Prefect + Dask)

Проектный каркас для аналитики торговых данных Московской биржи. Включает:
- Prefect 2.x flow, который тянет трейды с ISS API, считает агрегации на Dask и грузит в PostgreSQL.
- Dask scheduler/worker для параллельной обработки.
- Streamlit дашборд по витрине `mart.agg_trades`.
- Docker Compose для поднятия всего окружения.

## Быстрый старт
```bash
docker-compose up -d postgres
# подождите, пока Postgres инициализируется (healthcheck)
docker-compose up -d scheduler worker prefect agent streamlit
```
UI:
- Prefect UI: http://localhost:4200
- Dask dashboard: http://localhost:8787
- Streamlit: http://localhost:8501

## Настройка окружения
- Основные переменные: `DATABASE_URL` (по умолчанию `postgresql+psycopg2://prefect:prefect@postgres:5432/moex`). Dask сейчас работает на локальном scheduler threads, чтобы избежать обрывов соединения.
- База создается и схемы `raw`, `mart` поднимаются из `db/init.sql`.

## Запуск ETL
Запустите Flow локально (внутри контейнера agent, либо на хосте с установленными зависимостями):
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python flows/moex_flow.py  # пример запуска для тикеров SBER, GAZP
```
Регистрация в Prefect:
```bash
prefect config set PREFECT_API_URL=http://prefect:4200/api
prefect deployment build flows/moex_flow.py:moex_etl -n moex
prefect deployment apply moex_etl-deployment.yaml
prefect work-queue create default || true
```
После этого можно запускать из UI или CLI:
```bash
prefect flow-run create --deployment moex-etl/moex --params '{"tickers":["SBER","GAZP"],"trade_date":"2024-06-01"}'
```

## Структура
- `flows/moex_flow.py` — Prefect Flow с задачами extract/transform/load.
- `dask_jobs/processing.py` — вспомогательные функции Dask для агрегаций.
- `dashboards/app.py` — Streamlit дашборд по витрине.
- `db/init.sql` — схемы и таблицы `raw.trades`, `mart.agg_trades`.
- `docker-compose.yml` — Postgres, Prefect сервер/агент, Dask scheduler/worker, Streamlit.

## Что делает пайплайн
1. `extract_candles`: забирает минутные свечи (candles) по ISS API для тикера/даты.
2. `transform_candles`: чистит, считает rolling std по цене закрытия.
3. `load_to_postgres`: сохраняет сырые свечи в `raw.candles`, агрегации в `mart.agg_trades`.

## Проверка данных
- Дубликаты по `trade_id` фильтруются на этапе преобразования (dropna + уникальность).
- Индексы по времени и тикеру заданы в `db/init.sql` для быстрых выборок.

## Дальнейшие улучшения
- Добавить контроль пропусков минут и алерты (Prefect notifications).
- Поддержка MinIO для архивных сырых данных.
- Расширить дашборд (лидеры ликвидности, спайк-детектор).
- Настроить расписания в Prefect (cron/interval).
