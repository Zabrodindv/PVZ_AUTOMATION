"""
Отчет по привлечению ПВЗ
Источник: clickstream_b2c.events (ClickHouse DWH)
Лендинг: https://uzum.uz/ru/promo/pvz
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Добавляем родительскую директорию в path для импорта config
sys.path.insert(0, str(Path(__file__).parent.parent))

import clickhouse_connect
from config import CH_CONFIG


def get_client():
    return clickhouse_connect.get_client(
        host=CH_CONFIG['host'],
        port=CH_CONFIG['port'],
        username=CH_CONFIG['username'],
        password=CH_CONFIG['password'],
        secure=CH_CONFIG.get('secure', False)
    )


def get_pvz_daily_stats(report_date: datetime) -> dict:
    """
    Получить статистику по лендингу ПВЗ за конкретную дату
    - PAGE_VIEW: просмотры лендинга
    - BUTTON_CLICKED: клики по кнопке "Открыть пункт выдачи"
    """
    client = get_client()
    date_str = report_date.strftime('%Y-%m-%d')

    query = f"""
    SELECT
        countIf(DISTINCT install_id, event_type = 'PAGE_VIEW') as page_views,
        countIf(DISTINCT install_id, event_type = 'BUTTON_CLICKED' AND widget_name = 'PVZ_CREATE_APPLICATION') as button_clicks,
        round(countIf(DISTINCT install_id, event_type = 'BUTTON_CLICKED' AND widget_name = 'PVZ_CREATE_APPLICATION') * 100.0 /
              nullIf(countIf(DISTINCT install_id, event_type = 'PAGE_VIEW'), 0), 2) as conversion_rate
    FROM clickstream_b2c.events
    WHERE (widget_space_name = 'PVZ' OR (widget_space_name = 'LANDING' AND widget_name LIKE 'PVZ%'))
      AND event_type IN ('BUTTON_CLICKED', 'PAGE_VIEW')
      AND widget_name IN ('', 'PVZ_CREATE_APPLICATION', 'PVZ')
      AND toDate(received_at) = '{date_str}'
    """

    result = client.query(query)

    if result.result_rows:
        row = result.result_rows[0]
        return {
            'date': report_date,
            'page_views': row[0] or 0,
            'button_clicks': row[1] or 0,
            'conversion_rate': row[2] or 0.0
        }

    return {
        'date': report_date,
        'page_views': 0,
        'button_clicks': 0,
        'conversion_rate': 0.0
    }


def get_pvz_stats_by_platform(report_date: datetime) -> list:
    """
    Статистика по платформам (OS) за дату
    """
    client = get_client()
    date_str = report_date.strftime('%Y-%m-%d')

    query = f"""
    SELECT
        JSONExtractString(device_properties, 'os_name') AS os_name,
        countIf(DISTINCT install_id, event_type = 'PAGE_VIEW') as page_views,
        countIf(DISTINCT install_id, event_type = 'BUTTON_CLICKED' AND widget_name = 'PVZ_CREATE_APPLICATION') as button_clicks
    FROM clickstream_b2c.events
    WHERE (widget_space_name = 'PVZ' OR (widget_space_name = 'LANDING' AND widget_name LIKE 'PVZ%'))
      AND event_type IN ('BUTTON_CLICKED', 'PAGE_VIEW')
      AND widget_name IN ('', 'PVZ_CREATE_APPLICATION', 'PVZ')
      AND toDate(received_at) = '{date_str}'
    GROUP BY os_name
    ORDER BY page_views DESC
    """

    result = client.query(query)

    return [
        {
            'os_name': row[0] or 'Unknown',
            'page_views': row[1] or 0,
            'button_clicks': row[2] or 0
        }
        for row in result.result_rows
    ]


def get_pvz_stats_by_language(report_date: datetime) -> list:
    """
    Статистика по языкам за дату
    """
    client = get_client()
    date_str = report_date.strftime('%Y-%m-%d')

    query = f"""
    SELECT
        language,
        countIf(DISTINCT install_id, event_type = 'PAGE_VIEW') as page_views,
        countIf(DISTINCT install_id, event_type = 'BUTTON_CLICKED' AND widget_name = 'PVZ_CREATE_APPLICATION') as button_clicks
    FROM clickstream_b2c.events
    WHERE (widget_space_name = 'PVZ' OR (widget_space_name = 'LANDING' AND widget_name LIKE 'PVZ%'))
      AND event_type IN ('BUTTON_CLICKED', 'PAGE_VIEW')
      AND widget_name IN ('', 'PVZ_CREATE_APPLICATION', 'PVZ')
      AND toDate(received_at) = '{date_str}'
    GROUP BY language
    ORDER BY page_views DESC
    """

    result = client.query(query)

    return [
        {
            'language': row[0] or 'Unknown',
            'page_views': row[1] or 0,
            'button_clicks': row[2] or 0
        }
        for row in result.result_rows
    ]


def get_weekly_comparison(report_date: datetime) -> dict:
    """
    Сравнение с предыдущей неделей (тот же день недели)
    """
    client = get_client()
    date_str = report_date.strftime('%Y-%m-%d')
    prev_week_date = report_date - timedelta(days=7)
    prev_week_str = prev_week_date.strftime('%Y-%m-%d')

    query = f"""
    SELECT
        toDate(received_at) as date,
        countIf(DISTINCT install_id, event_type = 'PAGE_VIEW') as page_views,
        countIf(DISTINCT install_id, event_type = 'BUTTON_CLICKED' AND widget_name = 'PVZ_CREATE_APPLICATION') as button_clicks
    FROM clickstream_b2c.events
    WHERE (widget_space_name = 'PVZ' OR (widget_space_name = 'LANDING' AND widget_name LIKE 'PVZ%'))
      AND event_type IN ('BUTTON_CLICKED', 'PAGE_VIEW')
      AND widget_name IN ('', 'PVZ_CREATE_APPLICATION', 'PVZ')
      AND toDate(received_at) IN ('{date_str}', '{prev_week_str}')
    GROUP BY date
    ORDER BY date DESC
    """

    result = client.query(query)

    current = {'page_views': 0, 'button_clicks': 0}
    previous = {'page_views': 0, 'button_clicks': 0}

    for row in result.result_rows:
        if str(row[0]) == date_str:
            current = {'page_views': row[1] or 0, 'button_clicks': row[2] or 0}
        else:
            previous = {'page_views': row[1] or 0, 'button_clicks': row[2] or 0}

    # Расчёт изменения в процентах
    def calc_change(curr, prev):
        if prev == 0:
            return 0 if curr == 0 else 100
        return round((curr - prev) / prev * 100, 1)

    return {
        'current': current,
        'previous': previous,
        'previous_date': prev_week_date,
        'views_change': calc_change(current['page_views'], previous['page_views']),
        'clicks_change': calc_change(current['button_clicks'], previous['button_clicks'])
    }


def build_pvz_landing_report(report_date: datetime) -> dict:
    """
    Построить полный отчёт по привлечению ПВЗ за дату
    """
    print(f"Формирование отчёта за {report_date.date()}...")

    stats = get_pvz_daily_stats(report_date)
    print(f"  Просмотры: {stats['page_views']}, Клики: {stats['button_clicks']}")

    by_platform = get_pvz_stats_by_platform(report_date)
    by_language = get_pvz_stats_by_language(report_date)
    comparison = get_weekly_comparison(report_date)

    return {
        'date': report_date,
        'stats': stats,
        'by_platform': by_platform,
        'by_language': by_language,
        'comparison': comparison
    }


if __name__ == '__main__':
    # Тест: отчёт за вчера
    yesterday = datetime.now() - timedelta(days=1)
    report = build_pvz_landing_report(yesterday)

    print(f"\n{'='*60}")
    print(f"ОТЧЁТ ПО ПРИВЛЕЧЕНИЮ ПВЗ")
    print(f"Дата: {report['date'].strftime('%d.%m.%Y')}")
    print(f"{'='*60}")

    stats = report['stats']
    print(f"\nПросмотры лендинга:  {stats['page_views']:,}")
    print(f"Клики по кнопке:     {stats['button_clicks']:,}")
    print(f"Конверсия:           {stats['conversion_rate']:.2f}%")

    comp = report['comparison']
    print(f"\nСравнение с {comp['previous_date'].strftime('%d.%m.%Y')}:")
    print(f"  Просмотры: {comp['views_change']:+.1f}%")
    print(f"  Клики:     {comp['clicks_change']:+.1f}%")

    print(f"\nПо платформам:")
    for p in report['by_platform'][:5]:
        print(f"  {p['os_name']:<15}: {p['page_views']:,} просм., {p['button_clicks']:,} клик.")

    print(f"\nПо языкам:")
    for l in report['by_language']:
        print(f"  {l['language']:<10}: {l['page_views']:,} просм., {l['button_clicks']:,} клик.")
