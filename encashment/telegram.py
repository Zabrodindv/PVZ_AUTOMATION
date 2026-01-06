"""
Отправка отчета по инкассации ПВЗ в Telegram
"""

import os
import sys
import subprocess
import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Часовой пояс Ташкента
TZ_TASHKENT = pytz.timezone('Asia/Tashkent')

# Добавляем родительскую директорию в path для импорта config
sys.path.insert(0, str(Path(__file__).parent.parent))

from encashment.report import build_encashment_report

# Загружаем .env из родительской директории
load_dotenv(Path(__file__).parent.parent / ".env")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
LAST_RUN_FILE = Path.home() / ".encashment_last_run"

# Хосты для проверки VPN
VPN_HOSTS = [
    "wms-clickhouse.prod.um.internal",
    "dwh-clickhouse.prod.um.internal",
]


def check_vpn() -> bool:
    """Проверить подключение к VPN через ping внутренних хостов"""
    for host in VPN_HOSTS:
        try:
            result = subprocess.run(
                ["ping", "-c", "1", "-W", "2", host],
                capture_output=True,
                timeout=5
            )
            if result.returncode == 0:
                return True
        except (subprocess.TimeoutExpired, Exception):
            continue
    return False


def send_telegram_message(text: str, parse_mode: str = "HTML") -> bool:
    """Отправить сообщение в Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
    }
    try:
        response = requests.post(url, json=payload, timeout=30)
        return response.status_code == 200
    except Exception as e:
        print(f"Ошибка отправки в Telegram: {e}")
        return False


def categorize_reason(comment: str) -> str:
    """Категоризация причины несдачи"""
    if pd.isna(comment) or str(comment).strip() == '':
        return 'Без комментария'
    comment_lower = str(comment).lower()
    if any(x in comment_lower for x in ['электрич', 'свет', 'svet', 'свеч']):
        return 'Нет электричества'
    elif any(x in comment_lower for x in ['принтер', 'prentir', 'printer']):
        return 'Принтер не работает'
    elif any(x in comment_lower for x in ['инкассатор', 'inkasator', 'inkassa', 'не приехал', 'келмади', 'kelmadi']):
        return 'Инкассатор не приехал'
    elif any(x in comment_lower for x in ['мешк', 'мешок', 'qop', 'мишка', 'plomb']):
        return 'Нет мешков/пломб'
    elif any(x in comment_lower for x in ['касс']):
        return 'Проблемы с кассой'
    return 'Другое'


def format_report_for_telegram(report_df: pd.DataFrame, report_date: datetime) -> str:
    """Форматирование отчета для Telegram"""
    problems = report_df[report_df['conclusion'] == 'Не сдал, а должен был'].copy()
    no_schedule = report_df[report_df['conclusion'] == 'НЕТ ГРАФИКА']

    # Общая статистика
    unique_pvz = report_df['dp_shortname'].nunique()
    total = len(report_df)
    submitted = len(report_df[report_df['conclusion'] == 'Сдал по графику'])
    submitted_extra = len(report_df[report_df['conclusion'] == 'Сдал не по графику'])
    not_submitted = len(problems)
    not_required = len(report_df[report_df['conclusion'] == 'Не должен был сдавать'])
    no_schedule_count = len(no_schedule['dp_shortname'].unique()) if len(no_schedule) > 0 else 0

    # Количество ПВЗ, которые ДОЛЖНЫ были сдать сегодня
    scheduled_today = submitted + not_submitted

    # Проценты считаем от тех, кто должен был сдать
    submitted_pct = submitted / scheduled_today * 100 if scheduled_today > 0 else 0
    not_submitted_pct = not_submitted / scheduled_today * 100 if scheduled_today > 0 else 0

    # Заголовок
    lines = [
        f"<b>Инкассация ПВЗ</b>",
        f"Дата: {report_date.strftime('%d.%m.%Y')}",
        f"Всего ПВЗ: {unique_pvz}",
        f"📅 По графику сегодня: <b>{scheduled_today}</b>",
        "",
    ]

    # ПВЗ без графика
    if no_schedule_count > 0:
        no_schedule_pvz = sorted(no_schedule['dp_shortname'].unique())
        # Полный список
        pvz_str = ', '.join(no_schedule_pvz)
        lines.append(f"⚠️ <b>Нет графика ({no_schedule_count}):</b>")
        lines.append(f"<code>{pvz_str}</code>")
        lines.append("")

    if not_submitted == 0:
        lines.append("✅ Все ПВЗ сдали инкассацию по графику!")
    else:
        lines.append(f"❌ <b>Не сдали по графику: {not_submitted}</b>")
        lines.append("")

        # Группировка по причинам
        problems['reason_category'] = problems['comment'].apply(categorize_reason)
        reason_groups = problems.groupby('reason_category')['dp_shortname'].apply(list).to_dict()

        # Эмодзи для категорий
        emoji_map = {
            'Нет электричества': '🔴',
            'Инкассатор не приехал': '🟠',
            'Принтер не работает': '🟡',
            'Нет мешков/пломб': '🟣',
            'Проблемы с кассой': '🔵',
            'Без комментария': '⚪',
            'Другое': '⚫',
        }

        # Сортируем по количеству (от большего к меньшему)
        sorted_reasons = sorted(reason_groups.items(), key=lambda x: len(x[1]), reverse=True)

        for reason, pvz_list in sorted_reasons:
            emoji = emoji_map.get(reason, '⚪')
            count = len(pvz_list)
            # Полный список ПВЗ
            pvz_str = ', '.join(pvz_list)
            lines.append(f"{emoji} <b>{reason}</b> ({count}):")
            lines.append(f"<code>{pvz_str}</code>")
            lines.append("")

    # Общая статистика (проценты от тех кто должен был сдать)
    lines.append(f"✅ Сдали: {submitted}/{scheduled_today} ({submitted_pct:.1f}%)")
    lines.append(f"❌ Не сдали: {not_submitted}/{scheduled_today} ({not_submitted_pct:.1f}%)")

    # Дополнительно сдавшие не по графику
    if submitted_extra > 0:
        lines.append(f"📝 Сдали не по графику: {submitted_extra}")

    return "\n".join(lines)


def get_last_run_date() -> datetime | None:
    """Получить дату последнего успешного запуска"""
    if LAST_RUN_FILE.exists():
        try:
            date_str = LAST_RUN_FILE.read_text().strip()
            return datetime.strptime(date_str, "%Y-%m-%d")
        except Exception:
            pass
    return None


def save_last_run_date(date: datetime):
    """Сохранить дату последнего успешного запуска"""
    LAST_RUN_FILE.write_text(date.strftime("%Y-%m-%d"))


def get_missed_dates(last_run: datetime | None) -> list[datetime]:
    """Получить список пропущенных дат (по времени Ташкента)"""
    # Используем время Ташкента для определения "сегодня"
    now_tashkent = datetime.now(TZ_TASHKENT)
    today = now_tashkent.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    if last_run is None:
        # Если нет записи - отправляем только за сегодня
        return [today]

    missed = []
    current = last_run + timedelta(days=1)
    while current <= today:
        missed.append(current)
        current += timedelta(days=1)

    # Если список пустой, мы в вечернее время (после 20:00) И last_run < today
    # то отправляем за сегодня. Это покрывает случай когда скрипт запускается
    # в 22:00 того же дня, но отчёт ещё не отправлялся
    if not missed and now_tashkent.hour >= 20 and last_run < today:
        missed.append(today)

    return missed


def send_report_for_date(report_date: datetime) -> bool:
    """Сформировать и отправить отчет за конкретную дату"""
    print(f"Формирование отчета за {report_date.date()}...")

    date_from = report_date
    date_to = report_date + timedelta(days=1)

    try:
        report = build_encashment_report(date_from, date_to)

        if report.empty:
            print(f"  Нет данных за {report_date.date()}")
            return True  # Считаем успешным, просто нет данных

        message = format_report_for_telegram(report, report_date)

        if send_telegram_message(message):
            print(f"  Отчет за {report_date.date()} отправлен")
            return True
        else:
            print(f"  Ошибка отправки отчета за {report_date.date()}")
            return False

    except Exception as e:
        print(f"  Ошибка формирования отчета: {e}")
        return False


def main():
    """Основная функция"""
    now_tashkent = datetime.now(TZ_TASHKENT)
    print(f"=== Отчет по инкассации в Telegram ===")
    print(f"Время (Ташкент): {now_tashkent.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Проверка VPN
    print("Проверка VPN...")
    if not check_vpn():
        print("VPN не подключен! Выход.")
        return 1
    print("VPN подключен.")
    print()

    # Проверка пропущенных запусков
    last_run = get_last_run_date()
    if last_run:
        print(f"Последний успешный запуск: {last_run.date()}")
    else:
        print("Первый запуск")

    missed_dates = get_missed_dates(last_run)

    if not missed_dates:
        print("Нет пропущенных дат.")
        return 0

    print(f"Даты для отправки: {[d.strftime('%Y-%m-%d') for d in missed_dates]}")
    print()

    # Отправляем отчеты
    success_count = 0
    for date in missed_dates:
        if send_report_for_date(date):
            save_last_run_date(date)
            success_count += 1
        else:
            # Если не удалось отправить - прекращаем
            break

    print()
    print(f"Отправлено отчетов: {success_count}/{len(missed_dates)}")

    return 0 if success_count == len(missed_dates) else 1


if __name__ == "__main__":
    exit(main())
