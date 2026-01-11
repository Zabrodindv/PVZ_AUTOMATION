#!/usr/bin/env python3
"""
VPN Bot - Telegram бот для ручного управления Netbird VPN.

Команды:
    /vpn_status   - Показать статус VPN
    /vpn_restart  - Перезапустить демон netbird
    /vpn_reconnect - Переподключить VPN (down + up)

Доступ ограничен для ALLOWED_USER_ID.
"""

import os
import sys
import json
import logging
import subprocess
import time
import re
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler

import requests
from dotenv import load_dotenv

# Добавляем родительскую директорию для импорта
sys.path.insert(0, str(Path(__file__).parent.parent))

# Загружаем .env
load_dotenv(Path(__file__).parent.parent / ".env")

# Конфигурация
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ALLOWED_USER_ID = 862779466  # Только этот пользователь может управлять VPN

LOG_FILE = Path.home() / ".vpn_bot.log"

# Настройка логирования
logger = logging.getLogger("vpn_bot")
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=5 * 1024 * 1024,  # 5MB
    backupCount=2,
    encoding='utf-8'
)
file_handler.setFormatter(logging.Formatter(
    '[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
logger.addHandler(console_handler)


class TelegramBot:
    """Простой Telegram бот с long polling."""

    def __init__(self, token: str):
        self.token = token
        self.api_url = f"https://api.telegram.org/bot{token}"
        self.offset = 0

    def send_message(self, chat_id: int, text: str, parse_mode: str = "HTML") -> bool:
        """Отправить сообщение в Telegram."""
        try:
            response = requests.post(
                f"{self.api_url}/sendMessage",
                json={
                    "chat_id": chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                },
                timeout=30
            )
            return response.json().get("ok", False)
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {e}")
            return False

    def get_updates(self, timeout: int = 30) -> list:
        """Получить обновления через long polling."""
        try:
            response = requests.get(
                f"{self.api_url}/getUpdates",
                params={
                    "offset": self.offset,
                    "timeout": timeout,
                    "allowed_updates": ["message"],
                },
                timeout=timeout + 10
            )
            data = response.json()
            if data.get("ok"):
                return data.get("result", [])
        except requests.exceptions.Timeout:
            pass  # Нормально для long polling
        except Exception as e:
            logger.error(f"Ошибка получения обновлений: {e}")
            time.sleep(5)
        return []

    def process_updates(self, updates: list) -> None:
        """Обработать обновления."""
        for update in updates:
            self.offset = update["update_id"] + 1
            message = update.get("message")
            if message:
                self.handle_message(message)

    def handle_message(self, message: dict) -> None:
        """Обработать входящее сообщение."""
        user_id = message.get("from", {}).get("id")
        chat_id = message.get("chat", {}).get("id")
        text = message.get("text", "").strip()
        username = message.get("from", {}).get("username", "unknown")

        logger.info(f"Сообщение от {username} ({user_id}): {text}")

        # Проверка доступа
        if user_id != ALLOWED_USER_ID:
            logger.warning(f"Доступ запрещён для пользователя {user_id}")
            self.send_message(chat_id, "⛔ Доступ запрещён")
            return

        # Обработка команд
        if text == "/vpn_status":
            self.cmd_vpn_status(chat_id)
        elif text == "/vpn_restart":
            self.cmd_vpn_restart(chat_id)
        elif text == "/vpn_reconnect":
            self.cmd_vpn_reconnect(chat_id)
        elif text == "/start" or text == "/help":
            self.cmd_help(chat_id)
        elif text.startswith("/"):
            self.send_message(chat_id, "Неизвестная команда. Используй /help")

    def cmd_help(self, chat_id: int) -> None:
        """Показать справку."""
        text = """<b>VPN Bot - Управление Netbird</b>

/vpn_status - Показать статус VPN
/vpn_restart - Перезапустить демон
/vpn_reconnect - Переподключить (down + up)"""
        self.send_message(chat_id, text)

    def cmd_vpn_status(self, chat_id: int) -> None:
        """Показать статус VPN."""
        self.send_message(chat_id, "🔍 Проверяю статус VPN...")

        try:
            # Получаем статус netbird
            result = subprocess.run(
                ["netbird", "status", "-d"],
                capture_output=True,
                text=True,
                timeout=15
            )
            output = result.stdout

            # Парсим основные данные
            status_info = self._parse_vpn_status(output)

            # Проверяем ping
            ping_ok = self._check_ping()

            # Формируем ответ
            if status_info["connected"] and ping_ok:
                emoji = "✅"
                status_text = "Подключён"
            elif status_info["needs_login"]:
                emoji = "🔐"
                status_text = "Требуется авторизация"
            else:
                emoji = "❌"
                status_text = "Отключён"

            message = f"""{emoji} <b>VPN Status: {status_text}</b>

<b>Management:</b> {status_info['management']}
<b>Signal:</b> {status_info['signal']}
<b>Peers:</b> {status_info['peers']}
<b>NetBird IP:</b> {status_info['ip']}
<b>Ping внутренних хостов:</b> {'✅' if ping_ok else '❌'}

<i>Время: {datetime.now().strftime('%H:%M:%S')}</i>"""

            self.send_message(chat_id, message)

        except subprocess.TimeoutExpired:
            self.send_message(chat_id, "❌ Timeout при получении статуса")
        except Exception as e:
            self.send_message(chat_id, f"❌ Ошибка: {e}")

    def cmd_vpn_restart(self, chat_id: int) -> None:
        """Перезапустить демон netbird."""
        self.send_message(chat_id, "🔄 Перезапускаю демон netbird...")

        try:
            # Перезапуск демона
            result = subprocess.run(
                ["sudo", "systemctl", "restart", "netbird"],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                time.sleep(3)

                # Запускаем netbird up
                result_up = subprocess.run(
                    ["sudo", "netbird", "up"],
                    capture_output=True,
                    text=True,
                    timeout=15
                )

                output = result_up.stdout + result_up.stderr

                # Проверяем, нужна ли авторизация
                if "Already connected" in output:
                    self.send_message(chat_id, "✅ VPN подключён!")
                    return

                url_match = re.search(r'(https://[^\s]+user_code=[A-Z0-9-]+)', output)
                if url_match:
                    auth_url = url_match.group(1)
                    self.send_message(
                        chat_id,
                        f"🔐 <b>Требуется SSO авторизация</b>\n\n{auth_url}\n\n"
                        f"После авторизации выполни /vpn_restart ещё раз"
                    )
                    return

                # Проверяем итоговый статус
                time.sleep(3)
                if self._check_ping():
                    self.send_message(chat_id, "✅ VPN успешно перезапущен!")
                else:
                    self.send_message(chat_id, "⚠️ Демон перезапущен, но VPN не подключился")
            else:
                self.send_message(chat_id, f"❌ Ошибка перезапуска: {result.stderr}")

        except subprocess.TimeoutExpired:
            self.send_message(chat_id, "❌ Timeout при перезапуске")
        except Exception as e:
            self.send_message(chat_id, f"❌ Ошибка: {e}")

    def cmd_vpn_reconnect(self, chat_id: int) -> None:
        """Переподключить VPN (down + up)."""
        self.send_message(chat_id, "🔄 Переподключаю VPN...")

        try:
            # netbird down
            subprocess.run(
                ["sudo", "netbird", "down"],
                capture_output=True,
                timeout=15
            )
            time.sleep(2)

            # netbird up
            result = subprocess.run(
                ["sudo", "netbird", "up"],
                capture_output=True,
                text=True,
                timeout=15
            )

            output = result.stdout + result.stderr

            # Проверяем результат
            if "Already connected" in output:
                self.send_message(chat_id, "✅ VPN подключён!")
                return

            url_match = re.search(r'(https://[^\s]+user_code=[A-Z0-9-]+)', output)
            if url_match:
                auth_url = url_match.group(1)
                self.send_message(
                    chat_id,
                    f"🔐 <b>Требуется SSO авторизация</b>\n\n{auth_url}\n\n"
                    f"После авторизации выполни /vpn_restart"
                )
                return

            time.sleep(5)
            if self._check_ping():
                self.send_message(chat_id, "✅ VPN успешно переподключён!")
            else:
                self.send_message(chat_id, "⚠️ VPN не подключился. Попробуй /vpn_restart")

        except subprocess.TimeoutExpired:
            self.send_message(chat_id, "❌ Timeout. Попробуй /vpn_restart")
        except Exception as e:
            self.send_message(chat_id, f"❌ Ошибка: {e}")

    def _parse_vpn_status(self, output: str) -> dict:
        """Распарсить вывод netbird status."""
        info = {
            "connected": False,
            "needs_login": False,
            "management": "N/A",
            "signal": "N/A",
            "peers": "N/A",
            "ip": "N/A",
        }

        if "NeedsLogin" in output:
            info["needs_login"] = True
            return info

        # Management
        match = re.search(r'Management:\s*(\S+)', output)
        if match:
            info["management"] = match.group(1)
            if "Connected" in info["management"]:
                info["connected"] = True

        # Signal
        match = re.search(r'Signal:\s*(\S+)', output)
        if match:
            info["signal"] = match.group(1)

        # Peers count
        match = re.search(r'Peers count:\s*(\S+)', output)
        if match:
            info["peers"] = match.group(1)

        # NetBird IP
        match = re.search(r'NetBird IP:\s*(\S+)', output)
        if match:
            info["ip"] = match.group(1)

        return info

    def _check_ping(self) -> bool:
        """Проверить ping внутренних хостов."""
        hosts = [
            "wms-clickhouse.prod.um.internal",
            "dwh-clickhouse.prod.um.internal",
        ]
        for host in hosts:
            try:
                result = subprocess.run(
                    ["ping", "-c", "1", "-W", "2", host],
                    capture_output=True,
                    timeout=5
                )
                if result.returncode == 0:
                    return True
            except:
                continue
        return False

    def run(self) -> None:
        """Запустить бота."""
        logger.info("VPN Bot запущен")
        print(f"VPN Bot запущен. Разрешённый пользователь: {ALLOWED_USER_ID}")

        while True:
            try:
                updates = self.get_updates(timeout=30)
                if updates:
                    self.process_updates(updates)
            except KeyboardInterrupt:
                logger.info("Бот остановлен")
                break
            except Exception as e:
                logger.error(f"Ошибка в главном цикле: {e}")
                time.sleep(5)


def main():
    if not TELEGRAM_BOT_TOKEN:
        print("Ошибка: TELEGRAM_BOT_TOKEN не установлен")
        sys.exit(1)

    bot = TelegramBot(TELEGRAM_BOT_TOKEN)
    bot.run()


if __name__ == "__main__":
    main()
