#!/usr/bin/env python3
"""
VPN Monitor - автоматическая проверка и переподключение Netbird VPN

Проверяет состояние VPN каждые 5 минут через cron на Raspberry Pi.
При обнаружении отключения автоматически переподключается и отправляет уведомления в Telegram.
"""

import os
import sys
import json
import logging
import subprocess
import time
import re
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


def get_auth_url() -> str | None:
    """
    Получить URL для SSO авторизации через netbird login.

    Returns:
        str | None: URL для авторизации или None
    """
    try:
        logger.info("Выполняем: netbird login для получения SSO URL")
        result = subprocess.run(
            ["netbird", "login"],
            capture_output=True,
            text=True,
            timeout=30  # Увеличенный timeout для login
        )

        output = result.stdout + result.stderr

        # Ищем URL с user_code
        url_match = re.search(r'(https://[^\s]+user_code=[A-Z0-9-]+)', output)
        if url_match:
            return url_match.group(1)

        # Альтернативный паттерн - просто URL с device
        url_match = re.search(r'(https://[^\s]+/device\?user_code=[A-Z0-9-]+)', output)
        if url_match:
            return url_match.group(1)

    except subprocess.TimeoutExpired:
        logger.warning("Timeout при получении auth URL")
    except Exception as e:
        logger.error(f"Ошибка получения auth URL: {e}")

    return None


def reconnect_vpn(max_retries: int = 3) -> tuple[bool, int, str | None]:
    """
    Переподключить VPN через netbird down/up

    Returns:
        tuple[bool, int, str | None]: (успех, номер попытки, auth_url если требуется SSO)
    """
    auth_url = None

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

            output = result_up.stdout + result_up.stderr

            # Проверяем, требуется ли SSO авторизация
            if "SSO login" in output or "user_code" in output:
                # Извлекаем URL для авторизации
                url_match = re.search(r'(https://[^\s]+user_code=[A-Z0-9-]+)', output)
                if url_match:
                    auth_url = url_match.group(1)
                    logger.warning(f"Требуется SSO авторизация: {auth_url}")
                    return False, attempt, auth_url

            if result_up.returncode != 0:
                logger.warning(f"netbird up вернул код {result_up.returncode}: {result_up.stderr}")
                continue

            # 4. Ждем установления соединения
            logger.info("Ожидание установления соединения...")
            time.sleep(5)

            # 5. Проверяем подключение
            if is_vpn_connected():
                logger.info(f"VPN успешно переподключён (попытка {attempt})")
                return True, attempt, None
            else:
                logger.warning(f"VPN не подключился после попытки {attempt}")

        except subprocess.TimeoutExpired:
            logger.error(f"Timeout при переподключении (попытка {attempt})")
            # При timeout пробуем получить auth URL через netbird login
            if attempt == max_retries:
                logger.info("Пробуем получить auth URL через netbird login...")
                auth_url = get_auth_url()
                if auth_url:
                    logger.warning(f"Получен SSO URL: {auth_url}")
                    return False, attempt, auth_url
        except Exception as e:
            logger.error(f"Ошибка переподключения (попытка {attempt}): {e}")

        # Ждем перед следующей попыткой
        if attempt < max_retries:
            time.sleep(3)

    logger.error(f"Не удалось переподключить VPN после {max_retries} попыток")

    # Если auth_url не был получен во время попыток, пробуем получить его явно
    if auth_url is None:
        logger.info("Пробуем получить auth URL через netbird login...")
        auth_url = get_auth_url()
        if auth_url:
            logger.warning(f"Получен SSO URL: {auth_url}")

    return False, max_retries, auth_url


def send_telegram_alert(message: str, max_time: int = 900) -> bool:
    """
    Отправить уведомление в Telegram через curl (обход VPN/DNS проблем).
    Пробует отправить пока не получится или не истечёт max_time секунд.

    Args:
        message: Текст сообщения
        max_time: Максимальное время попыток в секундах (по умолчанию 15 минут)
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials не настроены")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
    })

    start_time = time.time()
    attempt = 0
    delay = 10  # Начальная пауза между попытками
    max_delay = 60  # Максимальная пауза

    while True:
        attempt += 1
        elapsed = time.time() - start_time

        if elapsed > max_time:
            logger.error(f"Превышено максимальное время отправки ({max_time} сек), попыток: {attempt - 1}")
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
                    logger.info(f"Telegram уведомление отправлено с {attempt}-й попытки")
                else:
                    logger.info("Telegram уведомление отправлено")
                return True
            else:
                error_desc = response.get('description', 'Unknown error')
                logger.warning(f"Telegram API ошибка: {error_desc}")
        except subprocess.TimeoutExpired:
            logger.warning(f"Попытка {attempt}: таймаут, повтор через {delay} сек...")
        except json.JSONDecodeError:
            logger.warning(f"Попытка {attempt}: некорректный ответ, повтор через {delay} сек...")
        except Exception as e:
            logger.warning(f"Попытка {attempt}: {e}, повтор через {delay} сек...")

        time.sleep(delay)
        delay = min(delay * 1.5, max_delay)  # Exponential backoff


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

    elif event_type == "auth_required":
        auth_url = kwargs.get("auth_url", "")
        return f"""🔐 <b>VPN Требует Авторизации</b>

Токен Netbird истёк.
Время: {now}

<b>Перейди по ссылке для авторизации:</b>
{auth_url}"""

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
        if previous_status in ("disconnected", "auth_required"):
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

        # Проверяем cooldown для уведомлений (30 минут)
        can_send_notification = should_send_notification(state, cooldown_minutes=30)

        # Если предыдущий статус был auth_required, пробуем только получить свежий URL
        if previous_status == "auth_required":
            logger.info("Предыдущий статус auth_required - получаем свежий SSO URL")
            auth_url = get_auth_url()
            if auth_url and can_send_notification:
                message = format_telegram_message("auth_required", auth_url=auth_url)
                send_telegram_alert(message)
                state["last_notification_time"] = datetime.now().isoformat()
            state["consecutive_failures"] += 1
            # Статус остаётся auth_required
        else:
            # Первое отключение или повторная попытка - пробуем переподключить
            # Отправляем уведомление об отключении (с учётом cooldown)
            if can_send_notification and previous_status != "disconnected":
                message = format_telegram_message("disconnect")
                send_telegram_alert(message)
                state["last_notification_time"] = datetime.now().isoformat()

            # Пытаемся переподключить
            success, attempt, auth_url = reconnect_vpn(max_retries=3)

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
            elif auth_url:
                # Требуется SSO авторизация
                logger.warning(f"Требуется SSO авторизация: {auth_url}")
                if can_send_notification:
                    message = format_telegram_message("auth_required", auth_url=auth_url)
                    send_telegram_alert(message)
                    state["last_notification_time"] = datetime.now().isoformat()
                state["last_status"] = "auth_required"
                state["consecutive_failures"] += 1
            else:
                # Неудачное переподключение
                logger.error("Не удалось переподключить VPN")
                if can_send_notification:
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
