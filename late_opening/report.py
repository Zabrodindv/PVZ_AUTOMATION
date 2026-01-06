"""
Отчет по своевременности открытия ПВЗ
Сравнение расписания с фактическим временем открытия смены
"""

import sys
from pathlib import Path

# Добавляем родительскую директорию в path для импорта config
sys.path.insert(0, str(Path(__file__).parent.parent))

import clickhouse_connect
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from config import CH_WMS_CONFIG, DB_CONFIG


def get_wms_client():
    return clickhouse_connect.get_client(
        host=CH_WMS_CONFIG['host'],
        port=CH_WMS_CONFIG['port'],
        username=CH_WMS_CONFIG['username'],
        password=CH_WMS_CONFIG['password'],
        secure=CH_WMS_CONFIG['secure']
    )


def get_pg_engine(dbname):
    password = quote_plus(DB_CONFIG['password'])
    url = f"postgresql://{DB_CONFIG['user']}:{password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{dbname}?sslmode={DB_CONFIG['sslmode']}"
    return create_engine(url)


def get_dp_schedule():
    """
    Получить расписание работы ПВЗ из ClickHouse WMS
    """
    client = get_wms_client()

    query = """
    SELECT
        short_name,
        key,
        time_from,
        time_to
    FROM bronze.delivery_db_delivery_point
    WHERE delivery_point_type = 'FRANCHISE'
      AND active = 1
      AND short_name NOT LIKE 'ip%'
    """

    result = client.query(query)

    df = pd.DataFrame(result.result_rows, columns=[
        'short_name', 'dp_key', 'time_from', 'time_to'
    ])

    # Очищаем short_name
    df['short_name'] = df['short_name'].str.strip()

    return df


def get_work_shifts(date_from, date_to):
    """
    Получить данные по открытию смен из PostgreSQL
    """
    engine = get_pg_engine('delivery-point')

    query = """
    SELECT
        DATE(time_opened AT TIME ZONE 'Asia/Tashkent') AS work_date,
        dp_key,
        MIN(time_opened AT TIME ZONE 'Asia/Tashkent') AS first_opened,
        (time_opened AT TIME ZONE 'Asia/Tashkent')::time AS open_time
    FROM work_shift
    WHERE time_opened AT TIME ZONE 'Asia/Tashkent' >= %(date_from)s
      AND time_opened AT TIME ZONE 'Asia/Tashkent' < %(date_to)s
    GROUP BY DATE(time_opened AT TIME ZONE 'Asia/Tashkent'), dp_key, (time_opened AT TIME ZONE 'Asia/Tashkent')::time
    """

    df = pd.read_sql(query, engine, params={'date_from': date_from, 'date_to': date_to})
    engine.dispose()

    return df


def parse_time(time_str):
    """Парсинг времени из строки HH:MM"""
    try:
        parts = time_str.split(':')
        return int(parts[0]) * 60 + int(parts[1])  # минуты от полуночи
    except:
        return None


def get_schedule_buckets():
    """
    Получить список уникальных времён открытия (бакетов)
    Возвращает отсортированный список времён в формате HH:MM
    """
    schedule_df = get_dp_schedule()
    buckets = schedule_df['time_from'].dropna().unique()
    return sorted(buckets)


def build_late_opening_report(date_from=None, date_to=None, deadline_time=None, schedule_time=None):
    """
    Построить отчет по опозданиям открытия ПВЗ

    Args:
        date_from: начало периода
        date_to: конец периода
        deadline_time: крайнее время открытия (если не указано - используется schedule_time)
        schedule_time: фильтр по расписанию открытия (например "09:00" или "10:00")
                      Если указано - отбираются только ПВЗ с этим временем открытия
                      и deadline_time = schedule_time (должны открыться к своему времени)
    """
    if date_from is None:
        date_from = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if date_to is None:
        date_to = date_from + timedelta(days=1)

    print(f"Период: {date_from.date()} - {date_to.date()}")

    # 1. Получаем расписание (для списка франчайзи)
    print("Загрузка списка ПВЗ...")
    schedule_df = get_dp_schedule()
    print(f"  Загружено {len(schedule_df)} франчайзи")

    # Режим сравнения: bucket (единый дедлайн) или individual (каждый ПВЗ со своим расписанием)
    use_individual_schedule = False

    # Фильтруем по времени открытия если указано
    if schedule_time:
        schedule_df = schedule_df[schedule_df['time_from'] == schedule_time].copy()
        print(f"  Фильтр по времени открытия {schedule_time}: {len(schedule_df)} ПВЗ")
        # Дедлайн = время открытия по расписанию
        if deadline_time is None:
            deadline_time = schedule_time
    else:
        # Режим final - сравниваем каждый ПВЗ с его собственным расписанием
        use_individual_schedule = True
        print(f"  Режим: индивидуальное сравнение с расписанием каждого ПВЗ")

    if not use_individual_schedule:
        deadline_minutes = parse_time(deadline_time)
        print(f"Крайнее время открытия: {deadline_time}")
    print()

    # 2. Получаем факт открытия смен
    print("Загрузка данных по открытию смен...")
    shifts_df = get_work_shifts(date_from, date_to)
    print(f"  Загружено {len(shifts_df)} записей")

    # Общее количество франчайзи (до джойна)
    total_franchise = len(schedule_df)

    # 3. Джойним расписание с фактом (LEFT JOIN чтобы видеть всех)
    result_df = schedule_df.merge(
        shifts_df,
        on='dp_key',
        how='left'
    )

    # Считаем открывшихся
    opened_df = result_df[result_df['open_time'].notna()]
    print(f"  Открылись: {opened_df['short_name'].nunique()} из {total_franchise}")

    if opened_df.empty:
        print("Нет данных по открытию смен")
        # Возвращаем пустой DataFrame с метаданными
        empty_df = pd.DataFrame()
        empty_df.attrs['total_pvz'] = total_franchise
        empty_df.attrs['opened_pvz'] = 0
        empty_df.attrs['late_pvz'] = 0
        empty_df.attrs['on_time_pvz'] = 0
        empty_df.attrs['not_opened_pvz'] = total_franchise
        empty_df.attrs['not_opened_list'] = sorted(schedule_df['short_name'].unique())
        empty_df.attrs['schedule_time'] = schedule_time
        empty_df.attrs['deadline_time'] = deadline_time
        return empty_df

    # 4. Вычисляем опоздание
    opened_df = opened_df.copy()

    if use_individual_schedule:
        # Сравниваем каждый ПВЗ с его собственным расписанием (time_from)
        def calculate_individual_delay(row):
            actual_time = row['open_time']
            scheduled_time = row['time_from']
            if pd.isna(actual_time) or pd.isna(scheduled_time):
                return None
            actual_minutes = actual_time.hour * 60 + actual_time.minute
            scheduled_minutes = parse_time(scheduled_time)
            if scheduled_minutes is None:
                return None
            return actual_minutes - scheduled_minutes

        opened_df['delay_minutes'] = opened_df.apply(calculate_individual_delay, axis=1)
        opened_df['scheduled_time'] = opened_df['time_from']  # Сохраняем расписание
    else:
        # Единый дедлайн для всех
        def calculate_delay(row):
            actual_time = row['open_time']
            if pd.isna(actual_time):
                return None
            actual_minutes = actual_time.hour * 60 + actual_time.minute
            return actual_minutes - deadline_minutes

        opened_df['delay_minutes'] = opened_df.apply(calculate_delay, axis=1)
        opened_df['scheduled_time'] = deadline_time

    # 5. Фильтруем опоздавших
    late_df = opened_df[opened_df['delay_minutes'] > 0].copy()
    late_df = late_df.sort_values('delay_minutes', ascending=False)

    # Форматируем вывод
    late_df['actual_time'] = late_df['open_time'].apply(
        lambda x: x.strftime('%H:%M') if pd.notna(x) else ''
    )
    late_df['delay_str'] = late_df['delay_minutes'].apply(
        lambda x: f"+{int(x)} мин" if pd.notna(x) else ''
    )

    # Статистика
    opened_pvz = opened_df['short_name'].nunique()
    late_pvz = late_df['short_name'].nunique()
    on_time_pvz = opened_pvz - late_pvz

    # Список не открывшихся ПВЗ
    opened_keys = set(opened_df['short_name'].unique())
    all_keys = set(schedule_df['short_name'].unique())
    not_opened_list = sorted(all_keys - opened_keys)
    not_opened_pvz = len(not_opened_list)

    report_df = late_df[[
        'short_name', 'work_date', 'scheduled_time', 'actual_time', 'delay_minutes', 'delay_str'
    ]].copy()

    # Метаданные
    report_df.attrs['total_pvz'] = total_franchise  # Все франчайзи
    report_df.attrs['opened_pvz'] = opened_pvz      # Открылись
    report_df.attrs['late_pvz'] = late_pvz          # Опоздали
    report_df.attrs['on_time_pvz'] = on_time_pvz    # Вовремя
    report_df.attrs['not_opened_pvz'] = not_opened_pvz  # Не открылись
    report_df.attrs['not_opened_list'] = not_opened_list  # Список не открывшихся
    report_df.attrs['schedule_time'] = schedule_time  # Бакет времени открытия
    report_df.attrs['deadline_time'] = deadline_time  # Дедлайн
    report_df.attrs['use_individual_schedule'] = use_individual_schedule  # Индивидуальное сравнение

    return report_df


def print_late_opening_report(df):
    """Вывести отчет по опозданиям"""
    # Получаем метаданные
    total_pvz = df.attrs.get('total_pvz', 0)
    late_pvz = df.attrs.get('late_pvz', len(df))
    on_time_pvz = df.attrs.get('on_time_pvz', 0)

    print("\n" + "=" * 80)
    print("ОТЧЕТ ПО СВОЕВРЕМЕННОСТИ ОТКРЫТИЯ ПВЗ")
    print("=" * 80)

    print(f"\nВсего ПВЗ в выборке: {total_pvz}")
    print(f"  ✅ Открылись вовремя: {on_time_pvz} ({on_time_pvz/total_pvz*100:.1f}%)" if total_pvz > 0 else "")
    print(f"  ❌ Опоздали: {late_pvz} ({late_pvz/total_pvz*100:.1f}%)" if total_pvz > 0 else "")

    if df.empty:
        print("\n🎉 Все ПВЗ открылись вовремя!")
        return

    print("\n" + "-" * 80)
    print("СПИСОК ОПОЗДАВШИХ")
    print("-" * 80)
    print()

    print(f"{'ПВЗ':<15} | {'Дата':<12} | {'Расписание':<10} | {'Факт':<10} | {'Опоздание':<12}")
    print("-" * 70)

    for _, row in df.iterrows():
        print(f"{row['short_name']:<15} | {str(row['work_date']):<12} | {row['scheduled_time']:<10} | {row['actual_time']:<10} | {row['delay_str']:<12}")

    # Статистика
    print("\n" + "=" * 80)
    print("СТАТИСТИКА")
    print("=" * 80)

    avg_delay = df['delay_minutes'].mean()
    max_delay = df['delay_minutes'].max()
    median_delay = df['delay_minutes'].median()

    print(f"\nСреднее опоздание:   {avg_delay:.0f} минут")
    print(f"Максимальное:        {max_delay:.0f} минут")
    print(f"Медиана:             {median_delay:.0f} минут")

    # Группировка по времени опоздания
    print("\nРаспределение по времени опоздания:")
    bins = [15, 30, 60, 120, float('inf')]
    labels = ['15-30 мин', '30-60 мин', '1-2 часа', '>2 часов']

    df['delay_group'] = pd.cut(df['delay_minutes'], bins=bins, labels=labels)
    group_counts = df['delay_group'].value_counts().sort_index()

    for group, count in group_counts.items():
        print(f"  {group:<15} {count:>4} ПВЗ")


if __name__ == '__main__':
    # Отчет за сегодня с дедлайном 09:40
    report = build_late_opening_report()
    print_late_opening_report(report)
