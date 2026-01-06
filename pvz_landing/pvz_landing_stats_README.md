# Статистика лендинга ПВЗ

Анализ трафика на лендинг ПВЗ и кликов по кнопке "Открыть пункт выдачи".

**Лендинг:** https://uzum.uz/ru/promo/pvz

## Описание

Скрипт анализирует clickstream-данные для лендинга франчайзинга ПВЗ:
- Просмотры страницы (PAGE_VIEW)
- Клики по кнопке "Открыть пункт выдачи" (BUTTON_CLICKED)
- Конверсия в разрезе платформ, языков, устройств
- UTM-источники трафика

## Источник данных

| Источник | База | Таблица | Данные |
|----------|------|---------|--------|
| ClickHouse DWH | clickstream_b2c | `events` | События на лендинге |

### Фильтры событий

```sql
widget_space_name = 'PVZ'
OR (widget_space_name = 'LANDING' AND widget_name LIKE 'PVZ%')
```

## Использование

```bash
cd /Users/denis/db_connection_project
python pvz_landing_stats.py
```

## Функции

| Функция | Описание |
|---------|----------|
| `get_pvz_stats(days_back)` | Статистика по дням (просмотры, клики) |
| `get_pvz_total_stats(days_back)` | Общая сводка за период |
| `get_pvz_funnel(date_from, date_to)` | Воронка: просмотр → клик |
| `get_pvz_conversion_by_platform(days_back)` | Конверсия по OS (Android/iOS/Web) |
| `get_pvz_conversion_by_language(days_back)` | Конверсия по языкам (ru/uz/en) |
| `get_pvz_conversion_by_device_type(days_back)` | Конверсия по устройствам (mobile/desktop/tablet) |
| `get_pvz_stats_by_utm(days_back)` | Статистика по UTM-источникам |

## Метрики

| Метрика | Описание |
|---------|----------|
| page_views | Уникальные пользователи, просмотревшие лендинг |
| button_clicks | Уникальные пользователи, кликнувшие "Открыть пункт выдачи" |
| conversion_rate | CR% = button_clicks / page_views × 100 |

## Пример вывода

```
ОБЩАЯ СВОДКА (за 7 дней)

  Период:                 7 дней
  Всего просмотров:       12,345 уник. пользователей
  Всего кликов:           1,234 уник. пользователей
  Конверсия:              10.00%
  Среднее в день:
    - просмотров:         1,763
    - кликов:             176
```

## Разрезы анализа

1. **По дням** — динамика трафика и кликов
2. **По платформам** — Android, iOS, Web
3. **По языкам** — ru, uz, en
4. **По типам устройств** — mobile, desktop, tablet
5. **По UTM** — источники трафика (utm_source, utm_campaign)

## Зависимости

- clickhouse_connect
