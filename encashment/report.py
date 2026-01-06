"""
Отчет по инкассации ПВЗ
Сравнение графика инкассации с фактическими данными
"""

import sys
from pathlib import Path

# Добавляем родительскую директорию в path для импорта config
sys.path.insert(0, str(Path(__file__).parent.parent))

import clickhouse_connect
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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
    """Создать SQLAlchemy engine для PostgreSQL"""
    password = quote_plus(DB_CONFIG['password'])
    url = f"postgresql://{DB_CONFIG['user']}:{password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{dbname}?sslmode={DB_CONFIG['sslmode']}"
    return create_engine(url)


def get_delivery_points_list(delivery_point_type: str = 'FRANCHISE'):
    """
    Получить список ПВЗ из ClickHouse WMS по типу

    Args:
        delivery_point_type: тип точки доставки ('FRANCHISE' или 'DELIVERY_POINT')
    """
    client = get_wms_client()

    query = """
    SELECT
        short_name,
        key
    FROM bronze.delivery_db_delivery_point
    WHERE delivery_point_type = %(dp_type)s
      AND active = 1
      AND short_name NOT LIKE 'ip%'
    """

    result = client.query(query, parameters={'dp_type': delivery_point_type})

    df = pd.DataFrame(result.result_rows, columns=['short_name', 'dp_key'])
    df['short_name'] = df['short_name'].str.strip()

    return df


# Алиас для обратной совместимости
def get_franchise_list():
    return get_delivery_points_list('FRANCHISE')


def get_encashment_schedule():
    """
    Получить график инкассации из ClickHouse WMS
    """
    client = get_wms_client()

    # Берём последнюю доступную дату (данные могут быть не за вчера)
    query = """
    SELECT
        dp_shortname,
        monday,
        tuesday,
        wednesday,
        thursday,
        friday,
        saturday,
        sunday,
        dp_closed
    FROM bronze.encashment_gsheet
    WHERE toDate(dt) = (SELECT max(toDate(dt)) FROM bronze.encashment_gsheet)
    """

    result = client.query(query)

    # Преобразуем в DataFrame
    df = pd.DataFrame(result.result_rows, columns=[
        'dp_shortname', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'is_closed'
    ])

    # Очищаем dp_shortname
    df['dp_shortname'] = df['dp_shortname'].str.strip().str.replace(' ', '')

    # Преобразуем строки 'да'/'Да' в boolean
    bool_cols = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun', 'is_closed']
    for col in bool_cols:
        df[col] = df[col].str.strip().str.lower() == 'да'

    # Формируем список дней инкассации (1=Пн, 7=Вс)
    def get_encashment_days(row):
        days = []
        if row['mon']: days.append(1)
        if row['tue']: days.append(2)
        if row['wed']: days.append(3)
        if row['thu']: days.append(4)
        if row['fri']: days.append(5)
        if row['sat']: days.append(6)
        if row['sun']: days.append(7)
        return days

    df['encashment_days'] = df.apply(get_encashment_days, axis=1)

    # Фильтруем закрытые ПВЗ
    df = df[~df['is_closed']]

    # Удаляем дубликаты по dp_shortname
    df = df.drop_duplicates(subset=['dp_shortname'], keep='first')

    return df[['dp_shortname', 'encashment_days']]


def get_delivery_points():
    """
    Получить справочник ПВЗ из PostgreSQL
    """
    engine = get_pg_engine('order')

    query = """
    SELECT key, short_name
    FROM delivery_point
    WHERE short_name IS NOT NULL
    """

    df = pd.read_sql(query, engine)
    engine.dispose()

    return df


def get_encashment_data(date_from, date_to, dp_map):
    """
    Получить фактические данные по инкассации из PostgreSQL
    dp_map: словарь {dp_key: short_name} из WMS
    """
    engine = get_pg_engine('delivery-point')

    query = """
    SELECT
        DATE(ws.time_opened AT TIME ZONE 'Asia/Tashkent') AS work_shift_day,
        CAST(ws.dp_key AS VARCHAR) AS dp_key,
        EXTRACT(ISODOW FROM DATE(ws.time_opened AT TIME ZONE 'Asia/Tashkent'))::INTEGER AS day_of_week,
        COALESCE(SUM(
            CASE
                WHEN cro.operation_type = 'WITHDRAWAL'
                     AND cro.category = 'ENCASHMENT'
                THEN cro.amount ELSE 0
            END
        ), 0) AS encashment_amount,
        MAX(bf.comment) AS comment
    FROM work_shift ws
    LEFT JOIN cash_register_operation cro
        ON ws.id = cro.work_shift_id
    LEFT JOIN bill_form bf
        ON bf.workshift_id = ws.id
    WHERE ws.time_opened AT TIME ZONE 'Asia/Tashkent' >= %(date_from)s
      AND ws.time_opened AT TIME ZONE 'Asia/Tashkent' < %(date_to)s
    GROUP BY 1, 2, 3
    """

    df = pd.read_sql(query, engine, params={'date_from': date_from, 'date_to': date_to})
    engine.dispose()

    # Добавляем short_name из WMS
    df['short_name'] = df['dp_key'].map(dp_map)

    return df


def build_encashment_report(date_from=None, date_to=None, delivery_point_type: str = 'FRANCHISE'):
    """
    Построить отчет по инкассации

    Args:
        date_from: начало периода
        date_to: конец периода
        delivery_point_type: тип точки доставки ('FRANCHISE' или 'DELIVERY_POINT')
    """
    if date_from is None:
        date_from = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    if date_to is None:
        date_to = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    print(f"Период: {date_from.date()} - {date_to.date()}")
    print()

    # 1. Получаем список ПВЗ из WMS (единый источник)
    print(f"Загрузка списка ПВЗ ({delivery_point_type}) из WMS...")
    franchise_df = get_delivery_points_list(delivery_point_type)
    print(f"  Загружено {len(franchise_df)} ПВЗ типа {delivery_point_type}")

    # Создаём маппинг dp_key -> short_name
    dp_map = {str(row['dp_key']): row['short_name'] for _, row in franchise_df.iterrows()}

    # 2. Получаем график инкассации
    print("Загрузка графика инкассации...")
    schedule_df = get_encashment_schedule()
    print(f"  Загружено {len(schedule_df)} ПВЗ с графиком")

    # 3. Получаем фактические данные
    print("Загрузка данных по инкассации...")
    encashment_df = get_encashment_data(date_from, date_to, dp_map)
    print(f"  Загружено {len(encashment_df)} записей")

    # 4. Начинаем с franchise_df, джойним с графиком (LEFT JOIN - чтобы увидеть без графика)
    result_df = franchise_df.merge(
        schedule_df,
        left_on='short_name',
        right_on='dp_shortname',
        how='left'
    )

    # Отмечаем ПВЗ без графика
    result_df['has_schedule'] = result_df['dp_shortname'].notna()
    no_schedule_count = (~result_df['has_schedule']).sum()
    print(f"  ПВЗ без графика инкассации: {no_schedule_count}")

    # 5. Джойним с фактическими данными
    result_df = result_df.merge(
        encashment_df[['short_name', 'work_shift_day', 'day_of_week', 'encashment_amount', 'comment']],
        on='short_name',
        how='inner'
    )

    print(f"  Matched с данными: {len(result_df)} записей")

    # Фильтруем только записи с данными
    result_df = result_df[result_df['day_of_week'].notna()]
    result_df['day_of_week'] = result_df['day_of_week'].astype(int)

    # 6. Определяем, был ли день по графику
    def is_scheduled(row):
        if not row['has_schedule']:
            return False
        days = row['encashment_days']
        if isinstance(days, list):
            return row['day_of_week'] in days
        return False

    result_df['is_scheduled_day'] = result_df.apply(is_scheduled, axis=1)

    # 7. Формируем вывод
    def get_conclusion(row):
        if not row['has_schedule']:
            return 'НЕТ ГРАФИКА'
        elif row['is_scheduled_day'] and row['encashment_amount'] > 0:
            return 'Сдал по графику'
        elif not row['is_scheduled_day'] and row['encashment_amount'] > 0:
            return 'Сдал не по графику'
        elif not row['is_scheduled_day'] and row['encashment_amount'] == 0:
            return 'Не должен был сдавать'
        else:
            return 'Не сдал, а должен был'

    result_df['conclusion'] = result_df.apply(get_conclusion, axis=1)

    # Форматируем дни инкассации
    result_df['encashment_days_str'] = result_df['encashment_days'].apply(
        lambda x: ', '.join(map(str, x)) if isinstance(x, list) and x else '-'
    )

    # Используем short_name вместо dp_shortname
    result_df['dp_shortname'] = result_df['short_name']

    final_df = result_df[[
        'dp_shortname', 'encashment_days_str', 'work_shift_day',
        'day_of_week', 'encashment_amount', 'conclusion', 'comment', 'has_schedule'
    ]]

    # Добавляем метаданные о типе ПВЗ
    final_df.attrs['delivery_point_type'] = delivery_point_type

    return final_df


def print_summary(df):
    """
    Вывести сводку по отчету
    """
    print("\n" + "=" * 80)
    print("СВОДКА ПО ИНКАССАЦИИ")
    print("=" * 80)

    # Общее количество уникальных ПВЗ
    unique_pvz = df['dp_shortname'].nunique()
    print(f"\nВсего ПВЗ: {unique_pvz}")

    summary = df['conclusion'].value_counts()
    total = len(df)

    print(f"Всего записей: {total}")
    print()
    for conclusion, count in summary.items():
        pct = count / total * 100
        print(f"  {conclusion:<30} {count:>6} ({pct:>5.1f}%)")

    # ПВЗ без графика
    no_schedule = df[df['conclusion'] == 'НЕТ ГРАФИКА']
    if len(no_schedule) > 0:
        no_schedule_pvz = no_schedule['dp_shortname'].unique()
        print(f"\n\n{'='*80}")
        print(f"⚠️  ПВЗ БЕЗ ГРАФИКА ИНКАССАЦИИ ({len(no_schedule_pvz)} шт)")
        print("=" * 80)
        print(", ".join(sorted(no_schedule_pvz)))

    # Проблемные ПВЗ (не сдали, а должны были)
    problems = df[df['conclusion'] == 'Не сдал, а должен был']
    if len(problems) > 0:
        print(f"\n\n{'='*120}")
        print("ПРОБЛЕМНЫЕ ПВЗ (не сдали, а должны были)")
        print("=" * 120)
        print(f"\n{'ПВЗ':<18} | {'Дата':<12} | {'День':<4} | {'Комментарий'}")
        print("-" * 120)
        for _, row in problems.iterrows():
            comment = row['comment'] if pd.notna(row['comment']) else ''
            print(f"{row['dp_shortname']:<18} | {str(row['work_shift_day']):<12} | {int(row['day_of_week']):<4} | {comment}")


def plot_encashment_report(df, save_path=None):
    """
    Построить интерактивные графики по отчету инкассации (Plotly)
    """
    # Создаем subplot с 2x2 графиками
    fig = make_subplots(
        rows=2, cols=2,
        specs=[[{"type": "pie"}, {"type": "bar"}],
               [{"type": "bar"}, {"type": "bar"}]],
        subplot_titles=(
            'Распределение по статусам',
            'Топ-15 ПВЗ по сумме инкассации',
            'Причины несдачи',
            'Сводка по статусам'
        ),
        vertical_spacing=0.12,
        horizontal_spacing=0.1
    )

    # Цвета для статусов
    colors_map = {
        'Сдал по графику': '#2ecc71',
        'Не должен был сдавать': '#3498db',
        'Не сдал, а должен был': '#e74c3c',
        'Сдал не по графику': '#f39c12',
        'НЕТ ГРАФИКА': '#9b59b6'  # фиолетовый
    }

    # 1. Круговая диаграмма по статусам
    summary = df['conclusion'].value_counts()
    pie_colors = [colors_map.get(x, '#95a5a6') for x in summary.index]
    fig.add_trace(
        go.Pie(
            labels=summary.index,
            values=summary.values,
            hole=0.3,
            marker_colors=pie_colors,
            textinfo='label+percent+value',
            textposition='outside',
            pull=[0.1 if 'Не сдал, а должен' in x else 0 for x in summary.index]
        ),
        row=1, col=1
    )

    # 2. Топ-15 ПВЗ по сумме инкассации
    top_pvz = df[df['encashment_amount'] > 0].nlargest(15, 'encashment_amount')
    fig.add_trace(
        go.Bar(
            y=top_pvz['dp_shortname'][::-1],
            x=top_pvz['encashment_amount'][::-1] / 1_000_000,
            orientation='h',
            marker_color='#3498db',
            text=[f'{v/1_000_000:.1f}M' for v in top_pvz['encashment_amount'][::-1]],
            textposition='outside',
            hovertemplate='%{y}<br>Сумма: %{x:.1f} млн<extra></extra>'
        ),
        row=1, col=2
    )

    # 3. Причины несдачи
    problems = df[df['conclusion'] == 'Не сдал, а должен был'].copy()
    if len(problems) > 0:
        problems['reason'] = problems['comment'].fillna('Без комментария').str[:30]
        reason_counts = problems['reason'].value_counts().head(10)
        fig.add_trace(
            go.Bar(
                y=reason_counts.index[::-1],
                x=reason_counts.values[::-1],
                orientation='h',
                marker_color='#e74c3c',
                text=reason_counts.values[::-1],
                textposition='outside',
                hovertemplate='%{y}<br>Количество: %{x}<extra></extra>'
            ),
            row=2, col=1
        )

    # 4. Сводка по статусам (столбчатая)
    status_data = df['conclusion'].value_counts()
    bar_colors = [colors_map.get(x, '#95a5a6') for x in status_data.index]
    fig.add_trace(
        go.Bar(
            x=status_data.index,
            y=status_data.values,
            marker_color=bar_colors,
            text=status_data.values,
            textposition='outside',
            hovertemplate='%{x}<br>Количество: %{y}<extra></extra>'
        ),
        row=2, col=2
    )

    # Настройка layout
    fig.update_layout(
        title={
            'text': f'<b>Отчет по инкассации ПВЗ</b><br><sub>Дата: {df["work_shift_day"].iloc[0]}</sub>',
            'x': 0.5,
            'font': {'size': 20}
        },
        showlegend=False,
        height=900,
        width=1400,
        template='plotly_white'
    )

    # Настройка осей
    fig.update_xaxes(title_text='Сумма (млн)', row=1, col=2)
    fig.update_xaxes(title_text='Количество ПВЗ', row=2, col=1)
    fig.update_yaxes(title_text='Количество ПВЗ', row=2, col=2)

    if save_path:
        # Сохраняем как HTML (интерактивный)
        html_path = save_path.replace('.png', '.html')
        fig.write_html(html_path)
        print(f"\nИнтерактивный график сохранен: {html_path}")

        # Пробуем сохранить как PNG (требует kaleido)
        try:
            fig.write_image(save_path, scale=2)
            print(f"Статичный график сохранен: {save_path}")
        except Exception:
            print("(PNG не сохранен - откройте HTML файл в браузере)")
    else:
        fig.show()

    return fig


if __name__ == '__main__':
    # Отчет за вчера
    report = build_encashment_report()

    print("\n" + "=" * 120)
    print("ДЕТАЛЬНЫЙ ОТЧЕТ")
    print("=" * 120)
    print(f"\n{'ПВЗ':<18} | {'Дни':<15} | {'Дата':<12} | {'ДН':<3} | {'Сумма':>12} | {'Статус':<25} | {'Комментарий'}")
    print("-" * 140)

    for _, row in report.head(50).iterrows():
        comment = row['comment'] if pd.notna(row['comment']) else ''
        print(f"{row['dp_shortname']:<18} | {row['encashment_days_str']:<15} | {str(row['work_shift_day']):<12} | {int(row['day_of_week']):<3} | {row['encashment_amount']:>12,.0f} | {row['conclusion']:<25} | {comment}")

    if len(report) > 50:
        print(f"\n... и ещё {len(report) - 50} записей")

    print_summary(report)

    # Строим графики
    plot_encashment_report(report, save_path='encashment_report.png')
