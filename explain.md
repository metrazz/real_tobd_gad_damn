# Explain: как работает проект MOEX ETL

## Архитектура
- **Источники данных**: ISS API Московской биржи, эндпоинт `candles.json` (минутные свечи).
- **Оркестрация**: Prefect 2.x flow `moex_etl` (файл `flows/moex_flow.py`).
- **Обработка**: Dask DataFrame (локальный scheduler threads, чтобы не зависеть от внешнего кластера).
- **Хранилище**: PostgreSQL, схемы `raw` (сырые свечи) и `mart` (витрина агрегаций).
- **Визуализация**: Streamlit (`dashboards/app.py`) поверх витрины `mart.agg_trades`.
- **Инфраструктура**: Docker Compose поднимает Postgres, Prefect server/agent, Dask scheduler/worker, Streamlit (`docker-compose.yml`).

## Поток данных (ETL)
1. **Extract** — задача `extract_candles` тянет свечи по каждому тикеру и дате через ISS API, возвращает pandas DataFrame.
2. **Transform** — задача `transform_candles`:
   - нормализует типы (datetime, numeric) в Dask DF;
   - очищает пустые строки;
   - добавляет минутный таймстамп `ts_minute`;
   - считает rolling std по `close` (15 минут, `vol_std`);
   - выдаёт сырые данные и витрину для загрузки.
3. **Load** — задача `load_to_postgres` пишет сырые свечи в `raw.candles`, витрину в `mart.agg_trades` через `to_sql`.
4. **Flow** — `moex_etl` параллелит extract/transform по тикерам, затем грузит результаты в БД.

## Модели данных
- `raw.candles`: `ts_start`, `ts_end`, `ticker`, `open`, `high`, `low`, `close`, `turnover`, `volume`, `ts_minute`, `vol_std`.
- `mart.agg_trades`: `ts_minute`, `ticker`, `open`, `high`, `low`, `close`, `volume`, `vol_std`.
Индексы по времени и тикеру заданы в `db/init.sql`.

## Сервисные компоненты
- **Postgres**: хранение сырых и агрегированных данных, инициализация через `db/init.sql`.
- **Prefect server + agent**: UI, оркестрация и выполнение flow.
- **Dask scheduler/worker**: подняты для нагрузки, но вычисления по умолчанию идут на локальных threads (fallback). Можно переключить на кластер, задав `DASK_SCHEDULER` и изменив код persist/compute.
- **Streamlit**: читает `mart.agg_trades`, строит графики цен, объёмов и волатильности, а также таблицу топовых объёмов.

## Запуск и эксплуатация (кратко)
1. Поднять Docker stack (`docker compose up ...`), инициализировать БД.
2. Собрать/применить деплой Prefect (`prefect deployment build/apply` в контейнере `agent`).
3. Запускать flow с параметрами тикеров и дат (только торговые дни).
4. Проверять загрузку через запросы к Postgres и смотреть дашборд на http://localhost:8501.

## Что ещё можно улучшить
- Добавить расписания Prefect (cron/interval) и нотификации.
- Подключить внешний Dask cluster и вернуть scheduler-адрес в конфиг.
- Расширить витрину (VWAP, спайк-метки, ликвидность) и дашборд.
- Добавить презентацию/архитектурную схему в репозиторий.
