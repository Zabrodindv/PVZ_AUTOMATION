"""
Отправка отчета по привлечению ПВЗ в Telegram
"""

import os
import sys
import subprocess
import pytz
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Часовой пояс Ташкента
TZ_TASHKENT = pytz.timezone('Asia/Tashkent')

# Добавляем родительскую директорию в path для импорта config
sys.path.insert(0, str(Path(__file__).parent.parent))

from pvz_landing.report import build_pvz_landing_report

# Загружаем .env из родительской директории
load_dotenv(Path(__file__).parent.parent / ".env")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = "-5122113963"  # Группа для отчётов по привлечению

# Хосты для проверки VPN
VPN_HOSTS = [
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


def send_telegram_message(text: str, parse_mode: str = "HTML", max_time: int = 900) -> bool:
    """
    Отправить сообщение в Telegram через curl (обход VPN/DNS проблем).
    Пробует отправить пока не получится или не истечёт max_time секунд.

    Args:
        text: Текст сообщения
        parse_mode: Режим парсинга (HTML/Markdown)
        max_time: Максимальное время попыток в секундах (по умолчанию 15 минут)
    """
    import json
    import time

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
    })

    start_time = time.time()
    attempt = 0
    delay = 10  # Начальная пауза между попытками
    max_delay = 60  # Максимальная пауза

    while True:
        attempt += 1
        elapsed = time.time() - start_time

        if elapsed > max_time:
            print(f"Превышено максимальное время отправки ({max_time} сек), попыток: {attempt - 1}")
            return False

        try:
            result = subprocess.run(
                ['curl', '-s', '-X', 'POST', url,
                 '-H', 'Content-Type: application/json',
                 '--connect-timeout', '10',
                 '-d', payload],
                capture_output=True,
                text=True,
                timeout=60
            )
            response = json.loads(result.stdout)
            if response.get('ok', False):
                if attempt > 1:
                    print(f"    Отправлено с {attempt}-й попытки")
                return True
            else:
                error_desc = response.get('description', 'Unknown error')
                print(f"    Telegram API ошибка: {error_desc}")
        except subprocess.TimeoutExpired:
            print(f"    Попытка {attempt}: таймаут, повтор через {delay} сек...")
        except json.JSONDecodeError:
            print(f"    Попытка {attempt}: некорректный ответ, повтор через {delay} сек...")
        except Exception as e:
            print(f"    Попытка {attempt}: {e}, повтор через {delay} сек...")

        time.sleep(delay)
        delay = min(delay * 1.5, max_delay)  # Exponential backoff


def format_report_for_telegram(report: dict) -> str:
    """Форматирование отчета для Telegram"""
    stats = report['stats']
    comp = report['comparison']
    report_date = report['date']

    # Эмодзи для изменений
    def trend_emoji(change):
        if change > 5:
            return "📈"
        elif change < -5:
            return "📉"
        return "➡️"

    # День недели на русском
    weekdays = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
    weekday = weekdays[report_date.weekday()]

    lines = [
        f"<b>Привлечение ПВЗ</b>",
        f"Дата: {report_date.strftime('%d.%m.%Y')} ({weekday})",
        "",
        f"<b>Лендинг uzum.uz/promo/pvz</b>",
        f"👀 Просмотры: <b>{stats['page_views']:,}</b>",
        f"👆 Клики: <b>{stats['button_clicks']:,}</b>",
        f"📊 Конверсия: <b>{stats['conversion_rate']:.1f}%</b>",
        "",
    ]

    # Сравнение с прошлой неделей
    prev_weekday = weekdays[comp['previous_date'].weekday()]
    lines.append(f"<b>vs {comp['previous_date'].strftime('%d.%m')} ({prev_weekday}):</b>")
    lines.append(f"{trend_emoji(comp['views_change'])} Просмотры: {comp['views_change']:+.1f}%")
    lines.append(f"{trend_emoji(comp['clicks_change'])} Клики: {comp['clicks_change']:+.1f}%")
    lines.append("")

    # По платформам (топ-3)
    if report['by_platform']:
        lines.append("<b>По платформам:</b>")
        for p in report['by_platform'][:3]:
            if p['page_views'] > 0:
                cr = round(p['button_clicks'] / p['page_views'] * 100, 1) if p['page_views'] > 0 else 0
                lines.append(f"• {p['os_name']}: {p['page_views']:,} → {p['button_clicks']:,} ({cr}%)")
        lines.append("")

    # По языкам
    if report['by_language']:
        lines.append("<b>По языкам:</b>")
        for l in report['by_language']:
            if l['page_views'] > 0:
                cr = round(l['button_clicks'] / l['page_views'] * 100, 1) if l['page_views'] > 0 else 0
                lines.append(f"• {l['language']}: {l['page_views']:,} → {l['button_clicks']:,} ({cr}%)")

    return "\n".join(lines)


def main():
    """Основная функция"""
    # Используем время Ташкента
    now_tashkent = datetime.now(TZ_TASHKENT)
    print(f"=== Отчет по привлечению ПВЗ в Telegram ===")
    print(f"Время (Ташкент): {now_tashkent.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Проверка VPN
    print("Проверка VPN...")
    if not check_vpn():
        print("VPN не подключен! Выход.")
        return 1
    print("VPN подключен.")
    print()

    # Отчёт за ВЧЕРА по времени Ташкента
    yesterday_tashkent = now_tashkent - timedelta(days=1)
    report_date = yesterday_tashkent.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    print(f"Формирование отчёта за {report_date.date()} (вчера по Ташкенту)...")

    try:
        report = build_pvz_landing_report(report_date)

        if report['stats']['page_views'] == 0:
            print(f"  Нет данных за {report_date.date()}")
            return 0

        message = format_report_for_telegram(report)

        if send_telegram_message(message):
            print(f"  Отчёт отправлен в группу {TELEGRAM_CHAT_ID}")
            return 0
        else:
            print(f"  Ошибка отправки отчёта")
            return 1

    except Exception as e:
        print(f"  Ошибка формирования отчёта: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())
