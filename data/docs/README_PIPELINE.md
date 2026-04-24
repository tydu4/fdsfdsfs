# Pipeline Runbook

## Главный запуск

Единая точка входа проекта находится в корне:

```bash
python main.py
```

Команда запускает локальный сайт на `localhost`. Через сайт можно:
- увидеть все этапы пайплайна;
- запустить полный цикл обработки;
- следить за статусом выполнения;
- открыть готовые таблицы, графики и презентационные материалы из `output`.

## Технический запуск без сайта

Если нужен только сам пайплайн:

```bash
python -m pipeline.run_pipeline
```

Параметры командной строки больше не используются. Настройки лежат в коде:
- `pipeline/settings.py`
- входные файлы: `ТЗ`
- результаты: `output`
- сайт: `127.0.0.1:8000` или ближайший свободный порт

## Состав пайплайна

1. `prepare_raw`: читает исходные Excel/CSV и формирует raw-слой.
2. `build_normalized`: собирает нормализованные таблицы и DQ-issues.
3. `build_mart`: строит leakage-safe витрину признаков по ЛС и месяцу.
4. `build_segmentation`: выполняет rule + KMeans сегментацию и hybrid business-gates.
5. `build_uplift_prototype`: оценивает эффект мер воздействия и рекомендует лучшую меру.
6. `build_presentation_viz`: генерирует графики для слайдов.
7. `build_presentation_notes`: готовит структуру доклада, speaker notes и чеклист.

## Основные результаты

- `output/raw/*.csv`
- `output/normalized/*.csv`
- `output/mart/mart_ls_month.csv`
- `output/segmentation/*.csv`
- `output/uplift/*.csv`
- `output/presentation/*.png`
- `output/presentation/*.md`
- `output/load_metrics_pipeline.json`
