#!/usr/bin/env python3
"""
VPN Monitor - автоматическая проверка и переподключение Netbird VPN

Проверяет состояние VPN каждые 5 минут через launchd.
При обнаружении отключения автоматически переподключается и отправляет уведомления в Telegram.
"""

import os
import sys
import json
import logging
import subprocess
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

# Добавляем родительскую директорию для импорта config
sys.path.insert(0, str(Path(__file__).parent.parent))

# Загружаем .env из родительской директории
load_dotenv(Path(__file__).parent.parent / ".env")

# Конфигурация
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("VPN_MONITOR_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
STATE_FILE = Path.home() / ".vpn_monitor_state.json"
LOG_FILE = Path.home() / ".vpn_monitor.log"

# Хосты для проверки подключения
VPN_HOSTS = [
    "wms-clickhouse.prod.um.internal",
    "dwh-clickhouse.prod.um.internal",
]

# Настройка логирования
logger = logging.getLogger("vpn_monitor")
logger.setLevel(logging.INFO)

# Rotating file handler: 10MB max, 3 backups
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=10 * 1024 * 1024,  # 10MB
    backupCount=3,
    encoding='utf-8'
)
file_handler.setFormatter(logging.Formatter(
    '[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
logger.addHandler(file_handler)

# Console handler для отладки
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
logger.addHandler(console_handler)


def check_vpn_netbird_status() -> bool:
    """Проверить статус netbird через команду netbird status"""
    try:
        result = subprocess.run(
            ["netbird", "status"],
            capture_output=True,
            text=True,
            timeout=10
        )

        # Netbird возвращает 0 при успешном выполнении
        if result.returncode == 0:
            output = result.stdout.lower()
            # Проверяем наличие индикаторов подключения
            if "connected" in output or "online" in output:
                return True

        return False
    except subprocess.TimeoutExpired:
        logger.warning("Netbird status timeout")
        return False
    except FileNotFoundError:
        logger.error("Netbird command not found")
        return False
    except Exception as e:
        logger.error(f"Ошибка проверки netbird status: {e}")
        return False


def check_vpn_connectivity() -> bool:
    """Проверить подключение через ping внутренних хостов"""
    for host in VPN_HOSTS:
        try:
            result = subprocess.run(
                ["ping", "-c", "1", "-W", "2", host],
                capture_output=True,
                timeout=5
            )
            if result.returncode == 0:
                logger.debug(f"Ping успешен: {host}")
                return True
        except subprocess.TimeoutExpired:
            logger.debug(f"Ping timeout: {host}")
            continue
        except Exception as e:
            logger.debug(f"Ping ошибка {host}: {e}")
            continue

    return False


def is_vpn_connected() -> bool:
    """Комбинированная проверка: netbird status И connectivity"""
    netbird_ok = check_vpn_netbird_status()
    connectivity_ok = check_vpn_connectivity()

    logger.debug(f"VPN check - Netbird: {netbird_ok}, Connectivity: {connectivity_ok}")

    # Оба должны быть в порядке для положительного результата
    return netbird_ok and connectivity_ok


def reconnect_vpn(max_retries: int = 3) -> tuple[bool, int]:
    """
    Переподключить VPN через netbird down/up

    Returns:
        tuple[bool, int]: (успех, номер попытки)
    """
    for attempt in range(1, max_retries + 1):
        logger.info(f"Попытка переподключения {attempt}/{max_retries}")

        try:
            # 1. Отключаем netbird
            logger.info("Выполняем: netbird down")
            result_down = subprocess.run(
                ["netbird", "down"],
                capture_output=True,
                text=True,
                timeout=15
            )

            if result_down.returncode != 0:
                logger.warning(f"netbird down вернул код {result_down.returncode}: {result_down.stderr}")

            # 2. Ждем 2 секунды
            time.sleep(2)

            # 3. Подключаем netbird
            logger.info("Выполняем: netbird up")
            result_up = subprocess.run(
                ["netbird", "up"],
                capture_output=True,
                text=True,
                timeout=15
            )

            if result_up.returncode != 0:
                logger.warning(f"netbird up вернул код {result_up.returncode}: {result_up.stderr}")
                continue

            # 4. Ждем установления соединения
            logger.info("Ожидание установления соединения...")
            time.sleep(5)

            # 5. Проверяем подключение
            if is_vpn_connected():
                logger.info(f"VPN успешно переподключён (попытка {attempt})")
                return True, attempt
            else:
                logger.warning(f"VPN не подключился после попытки {attempt}")

        except subprocess.TimeoutExpired:
            logger.error(f"Timeout при переподключении (попытка {attempt})")
        except Exception as e:
            logger.error(f"Ошибка переподключения (попытка {attempt}): {e}")

        # Ждем перед следующей попыткой
        if attempt < max_retries:
            time.sleep(3)

    logger.error(f"Не удалось переподключить VPN после {max_retries} попыток")
    return False, max_retries


def send_telegram_alert(message: str) -> bool:
    """Отправить уведомление в Telegram"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials не настроены")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        if response.status_code == 200:
            logger.info("Telegram уведомление отправлено")
            return True
        else:
            logger.warning(f"Telegram API вернул код {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")
        return False


def load_state() -> dict:
    """Загрузить состояние из JSON файла"""
    default_state = {
        "last_check": None,
        "last_status": "unknown",
        "last_notification_time": None,
        "reconnect_count": 0,
        "consecutive_failures": 0,
    }

    if not STATE_FILE.exists():
        return default_state

    try:
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            state = json.load(f)
            # Merge with defaults to handle new fields
            return {**default_state, **state}
    except Exception as e:
        logger.error(f"Ошибка загрузки состояния: {e}")
        return default_state


def save_state(state: dict) -> None:
    """Сохранить состояние в JSON файл"""
    try:
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
        logger.debug("Состояние сохранено")
    except Exception as e:
        logger.error(f"Ошибка сохранения состояния: {e}")


def should_send_notification(state: dict, cooldown_minutes: int = 30) -> bool:
    """Проверить, нужно ли отправлять уведомление (cooldown для предотвращения спама)"""
    last_notif = state.get("last_notification_time")

    if last_notif is None:
        return True

    try:
        last_time = datetime.fromisoformat(last_notif)
        now = datetime.now()
        elapsed = (now - last_time).total_seconds() / 60  # в минутах

        return elapsed >= cooldown_minutes
    except Exception as e:
        logger.warning(f"Ошибка проверки cooldown: {e}")
        return True


def format_telegram_message(event_type: str, **kwargs) -> str:
    """Форматировать сообщение для Telegram"""
    now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    if event_type == "disconnect":
        return f"""⚠️ <b>VPN Отключение</b>

Netbird VPN не доступен
Время: {now}
Попытка переподключения..."""

    elif event_type == "reconnect_success":
        attempt = kwargs.get("attempt", 1)
        max_attempts = kwargs.get("max_attempts", 3)
        return f"""✅ <b>VPN Восстановлен</b>

Netbird успешно переподключён
Попытка: {attempt}/{max_attempts}
Время: {now}"""

    elif event_type == "reconnect_failure":
        attempts = kwargs.get("attempts", 3)
        return f"""❌ <b>VPN Ошибка Переподключения</b>

Не удалось восстановить Netbird
Попыток: {attempts}/{attempts}
Время: {now}

Требуется ручное вмешательство!"""

    elif event_type == "recovered":
        return f"""✅ <b>VPN Восстановлен Автоматически</b>

Netbird VPN снова доступен
Время: {now}"""

    else:
        return f"VPN Monitor: {event_type} в {now}"


def main():
    """Основная функция мониторинга"""
    logger.info("=== Запуск проверки VPN ===")

    # Загружаем состояние
    state = load_state()

    # Проверяем VPN
    vpn_connected = is_vpn_connected()
    current_status = "connected" if vpn_connected else "disconnected"
    previous_status = state.get("last_status", "unknown")

    logger.info(f"Статус VPN: {current_status} (предыдущий: {previous_status})")

    # Обработка состояний
    if vpn_connected:
        # VPN подключён
        if previous_status == "disconnected":
            # Восстановление после отключения (без нашего вмешательства)
            logger.info("VPN восстановился автоматически")
            message = format_telegram_message("recovered")
            send_telegram_alert(message)
            state["last_notification_time"] = datetime.now().isoformat()
            state["consecutive_failures"] = 0

        state["last_status"] = "connected"
        state["reconnect_count"] = 0

    else:
        # VPN отключён
        logger.warning("VPN отключён - начинаем процесс переподключения")

        # Отправляем уведомление об отключении (с учётом cooldown)
        if should_send_notification(state, cooldown_minutes=30):
            message = format_telegram_message("disconnect")
            send_telegram_alert(message)
            state["last_notification_time"] = datetime.now().isoformat()

        # Пытаемся переподключить
        success, attempt = reconnect_vpn(max_retries=3)

        if success:
            # Успешное переподключение
            logger.info("VPN успешно переподключён")
            message = format_telegram_message(
                "reconnect_success",
                attempt=attempt,
                max_attempts=3
            )
            send_telegram_alert(message)
            state["last_notification_time"] = datetime.now().isoformat()
            state["last_status"] = "connected"
            state["reconnect_count"] += 1
            state["consecutive_failures"] = 0
        else:
            # Неудачное переподключение
            logger.error("Не удалось переподключить VPN")
            message = format_telegram_message("reconnect_failure", attempts=3)
            send_telegram_alert(message)
            state["last_notification_time"] = datetime.now().isoformat()
            state["last_status"] = "disconnected"
            state["consecutive_failures"] += 1

    # Обновляем время последней проверки
    state["last_check"] = datetime.now().isoformat()

    # Сохраняем состояние
    save_state(state)

    logger.info("=== Проверка завершена ===\n")

    # Возвращаем код: 0 если всё ОК, 1 если VPN отключён
    return 0 if vpn_connected else 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)
