#!/bin/bash
#
# Запуск VPN Monitor с логированием
# Для использования с launchd
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$HOME/.vpn_monitor_wrapper.log"
PYTHON_PATH="/opt/anaconda3/bin/python3"

# Функция логирования
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Основной скрипт
main() {
    log "=== VPN Monitor Check ==="

    # Переходим в директорию со скриптом
    cd "$SCRIPT_DIR" || {
        log "Ошибка: не удалось перейти в $SCRIPT_DIR"
        exit 1
    }

    # Активируем виртуальное окружение если есть (в родительской директории)
    PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
    if [ -f "$PROJECT_DIR/venv/bin/activate" ]; then
        source "$PROJECT_DIR/venv/bin/activate"
        PYTHON_PATH="$PROJECT_DIR/venv/bin/python"
        log "Использую venv: $PYTHON_PATH"
    fi

    # Запускаем Python скрипт
    log "Запуск monitor.py..."
    "$PYTHON_PATH" "$SCRIPT_DIR/monitor.py"

    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        log "Monitor check completed: VPN OK"
    else
        log "Monitor check completed: VPN issue detected (код: $exit_code)"
    fi

    log "=== Завершение ==="
    echo "" >> "$LOG_FILE"

    exit $exit_code
}

main "$@"
