"""
Отправка отчета по своевременности открытия ПВЗ в Telegram

Режимы работы:
- bucket: отчет по конкретному бакету времени открытия (например --schedule 09:00)
- final: контрольный замер по всем ПВЗ
"""

import os
import sys
import socket
import argparse
import subprocess

# Принудительно использовать IPv4 для requests (IPv6 не работает на Raspberry Pi)
_original_getaddrinfo = socket.getaddrinfo
def _getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
    return _original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
socket.getaddrinfo = _getaddrinfo_ipv4

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

from late_opening.report import build_late_opening_report, get_schedule_buckets

# Загружаем .env из родительской директории
load_dotenv(Path(__file__).parent.parent / ".env")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DELIVERY_POINT_CHAT_ID = os.getenv("DELIVERY_POINT_CHAT_ID")

# Маппинг типов ПВЗ на chat_id
CHAT_IDS = {
    'FRANCHISE': TELEGRAM_CHAT_ID,
    'DELIVERY_POINT': DELIVERY_POINT_CHAT_ID,
}

# Названия типов для отображения
DP_TYPE_NAMES = {
    'FRANCHISE': 'Франчайзи',
    'DELIVERY_POINT': 'Собственные',
}

# Файл для отслеживания последнего успешного контрольного запуска
LAST_RUN_FILE = Path.home() / ".late_opening_last_run"

# Время контрольного замера (Ташкент)
FINAL_CHECK_HOUR = 11
FINAL_CHECK_MINUTE = 0

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


def get_last_run_date() -> datetime | None:
    """Получить дату последнего успешного контрольного запуска"""
    if not LAST_RUN_FILE.exists():
        return None
    try:
        date_str = LAST_RUN_FILE.read_text().strip()
        return datetime.strptime(date_str, "%Y-%m-%d")
    except:
        return None


def save_last_run_date(date: datetime):
    """Сохранить дату последнего успешного контрольного запуска"""
    LAST_RUN_FILE.write_text(date.strftime("%Y-%m-%d"))


def should_send_final_report(now_tashkent: datetime) -> bool:
    """
    Проверить, нужно ли отправить контрольный отчет.
    Возвращает True если:
    - Сейчас после времени контрольного замера (11:00)
    - И сегодня еще не отправляли контрольный отчет
    """
    today = now_tashkent.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    # Проверяем, прошло ли время контрольного замера
    current_minutes = now_tashkent.hour * 60 + now_tashkent.minute
    final_check_minutes = FINAL_CHECK_HOUR * 60 + FINAL_CHECK_MINUTE

    if current_minutes < final_check_minutes:
        # Еще рано для контрольного замера
        return False

    # Проверяем, отправляли ли уже сегодня
    last_run = get_last_run_date()
    if last_run is not None and last_run >= today:
        # Уже отправляли сегодня
        return False

    return True


def send_telegram_message(text: str, chat_id: str = None, parse_mode: str = "HTML") -> bool:
    """Отправить сообщение в Telegram через curl (обход VPN/DNS проблем)"""
    import json

    if chat_id is None:
        chat_id = TELEGRAM_CHAT_ID

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": parse_mode,
    })

    try:
        result = subprocess.run(
            ['curl', '-s', '-X', 'POST', url,
             '-H', 'Content-Type: application/json',
             '-d', payload],
            capture_output=True,
            text=True,
            timeout=60
        )
        response = json.loads(result.stdout)
        return response.get('ok', False)
    except Exception as e:
        print(f"Ошибка отправки в Telegram: {e}")
        return False


def format_report_for_telegram(report_df: pd.DataFrame, report_date: datetime, check_time: str = None, mode: str = "final") -> str:
    """
    Форматирование отчета для Telegram

    Args:
        report_df: DataFrame с отчетом
        report_date: дата отчета
        check_time: время проверки
        mode: режим - "bucket" (по бакету) или "final" (контрольный)
    """
    # Получаем метаданные
    total_pvz = report_df.attrs.get('total_pvz', 0)
    opened_pvz = report_df.attrs.get('opened_pvz', 0)
    late_pvz = report_df.attrs.get('late_pvz', len(report_df))
    on_time_pvz = report_df.attrs.get('on_time_pvz', 0)
    not_opened_pvz = report_df.attrs.get('not_opened_pvz', 0)
    schedule_time = report_df.attrs.get('schedule_time')
    use_individual = report_df.attrs.get('use_individual_schedule', False)

    # Получаем тип ПВЗ
    dp_type = report_df.attrs.get('delivery_point_type', 'FRANCHISE')
    dp_type_name = DP_TYPE_NAMES.get(dp_type, dp_type)

    # Определяем время проверки
    if check_time is None:
        check_time = datetime.now().strftime('%H:%M')

    # Заголовок зависит от режима
    if mode == "bucket" and schedule_time:
        title = f"<b>Открытие ПВЗ ({dp_type_name}, расписание {schedule_time})</b>"
        lines = [
            title,
            f"Дата: {report_date.strftime('%d.%m.%Y')} | Проверка: {check_time}",
            "",
            f"📅 По расписанию на {schedule_time}: <b>{total_pvz}</b> ПВЗ",
            f"✅ Открылись вовремя: {on_time_pvz}",
        ]
    elif mode == "final":
        title = f"<b>Открытие ПВЗ ({dp_type_name}, контрольный)</b>"
        lines = [
            title,
            f"Дата: {report_date.strftime('%d.%m.%Y')} | Проверка: {check_time}",
            "",
            f"Всего ПВЗ: <b>{total_pvz}</b>",
            f"✅ Открылись вовремя: {on_time_pvz}",
        ]
    else:
        title = f"<b>Открытие ПВЗ ({dp_type_name})</b>"
        lines = [
            title,
            f"Дата: {report_date.strftime('%d.%m.%Y')} | Проверка: {check_time}",
            "",
            f"Всего ПВЗ: <b>{total_pvz}</b>",
            f"✅ Открылись вовремя: {on_time_pvz}",
        ]

    # Для bucket режима: показываем только "ещё не открылись"
    # Для final режима: показываем опоздавших с точным временем
    if mode == "bucket":
        # В bucket режиме не может быть "опоздавших" - только "ещё не открылись"
        if not_opened_pvz > 0:
            not_opened_list = report_df.attrs.get('not_opened_list', [])
            lines.append(f"⚠️ <b>Ещё не открылись ({not_opened_pvz}):</b>")
            pvz_str = ', '.join(not_opened_list)
            lines.append(f"<code>{pvz_str}</code>")
    else:
        # В final режиме показываем опоздавших с точным временем открытия
        if late_pvz > 0:
            lines.append(f"❌ Опоздали: <b>{late_pvz}</b>")
            lines.append("")
            lines.append("<b>Опоздавшие ПВЗ:</b>")
            for _, row in report_df.iterrows():
                scheduled = row.get('scheduled_time', '?')
                actual = row.get('actual_time', '?')
                lines.append(f"• {row['short_name']}: открытие {actual} (график {scheduled})")

        # Если есть не открывшиеся к контрольному времени
        if not_opened_pvz > 0:
            not_opened_list = report_df.attrs.get('not_opened_list', [])
            lines.append("")
            lines.append(f"⚠️ <b>Ещё не открылись ({not_opened_pvz}):</b>")
            pvz_str = ', '.join(not_opened_list)
            lines.append(f"<code>{pvz_str}</code>")

    return "\n".join(lines)


def parse_args():
    """Парсинг аргументов командной строки"""
    parser = argparse.ArgumentParser(
        description='Отчет по открытию ПВЗ в Telegram'
    )
    parser.add_argument(
        '--mode',
        choices=['bucket', 'final'],
        default='final',
        help='Режим: bucket (по времени открытия) или final (контрольный по всем)'
    )
    parser.add_argument(
        '--schedule',
        type=str,
        help='Время открытия для режима bucket (например 09:00 или 10:00)'
    )
    parser.add_argument(
        '--deadline',
        type=str,
        help='Крайнее время открытия (по умолчанию = schedule для bucket, 09:40 для final)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Принудительно отправить контрольный отчет, игнорируя проверку времени/last_run'
    )
    return parser.parse_args()


def main():
    """Основная функция"""
    args = parse_args()

    # Используем время Ташкента
    now = datetime.now(TZ_TASHKENT)
    check_time = now.strftime('%H:%M')

    print(f"=== Отчет по открытию ПВЗ в Telegram ===")
    print(f"Время проверки (Ташкент): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Режим: {args.mode}")
    if args.schedule:
        print(f"Бакет времени: {args.schedule}")
    print()

    # Для режима final проверяем, нужно ли отправлять
    if args.mode == 'final':
        last_run = get_last_run_date()
        print(f"Последний контрольный отчет: {last_run.strftime('%Y-%m-%d') if last_run else 'никогда'}")

        if not args.force and not should_send_final_report(now):
            print("Контрольный отчет за сегодня уже отправлен или ещё рано. Пропуск.")
            return 0

    # Проверка VPN
    print("Проверка VPN...")
    if not check_vpn():
        print("VPN не подключен! Выход.")
        return 1
    print("VPN подключен.")
    print()

    # Формируем отчет за сегодня (по времени Ташкента)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    date_from = today
    date_to = today + timedelta(days=1)

    # Типы ПВЗ для обработки
    dp_types = ['FRANCHISE', 'DELIVERY_POINT']

    overall_success = True

    for dp_type in dp_types:
        dp_type_name = DP_TYPE_NAMES.get(dp_type, dp_type)

        # Проверяем, настроен ли chat_id для этого типа
        chat_id = CHAT_IDS.get(dp_type)
        if not chat_id:
            print(f"ПРЕДУПРЕЖДЕНИЕ: Chat ID не настроен для {dp_type}, пропуск")
            continue

        print(f"\n--- Обработка типа: {dp_type_name} ---")
        print(f"Формирование отчета за {today.date()}...")

        try:
            # Определяем параметры в зависимости от режима
            if args.mode == 'bucket':
                if not args.schedule:
                    print("Ошибка: для режима bucket требуется --schedule")
                    return 1
                report = build_late_opening_report(
                    date_from, date_to,
                    deadline_time=args.deadline,
                    schedule_time=args.schedule,
                    delivery_point_type=dp_type
                )
            else:  # final - индивидуальное сравнение с расписанием каждого ПВЗ
                report = build_late_opening_report(
                    date_from, date_to,
                    deadline_time=args.deadline,  # None = индивидуальное сравнение
                    schedule_time=None,
                    delivery_point_type=dp_type
                )

            total_pvz = report.attrs.get('total_pvz', 0)
            if total_pvz == 0:
                print(f"  Нет данных ({dp_type_name}) за {today.date()}")
                continue

            message = format_report_for_telegram(report, today, check_time, mode=args.mode)

            if send_telegram_message(message, chat_id=chat_id):
                print(f"  Отчет ({dp_type_name}) отправлен")
            else:
                print(f"  Ошибка отправки отчета ({dp_type_name})")
                overall_success = False

        except Exception as e:
            print(f"  Ошибка формирования отчета ({dp_type_name}): {e}")
            import traceback
            traceback.print_exc()
            overall_success = False

    # Сохраняем дату успешной отправки контрольного отчета
    if args.mode == 'final' and overall_success:
        save_last_run_date(today)
        print(f"\nДата контрольного отчета сохранена: {today.date()}")

    return 0 if overall_success else 1


if __name__ == "__main__":
    exit(main())
