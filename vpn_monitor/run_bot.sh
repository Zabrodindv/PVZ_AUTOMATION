#!/bin/bash
# Запуск VPN Bot
# Использование: ./run_bot.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"
source venv/bin/activate

exec python3 vpn_monitor/vpn_bot.py
