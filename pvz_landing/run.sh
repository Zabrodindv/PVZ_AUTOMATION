#!/bin/bash
#
# Запуск отчета по привлечению ПВЗ с проверкой VPN
# Для использования с launchd/cron
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$HOME/.pvz_landing_report.log"
PYTHON_PATH="/opt/anaconda3/bin/python3"

# Функция логирования
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Проверка VPN через ping
check_vpn() {
    local hosts=("dwh-clickhouse.prod.um.internal")

    for host in "${hosts[@]}"; do
        if ping -c 1 -W 2 "$host" &>/dev/null; then
            return 0
        fi
    done
    return 1
}

# Ожидание VPN с таймаутом
wait_for_vpn() {
    local max_attempts=30  # 30 попыток по 10 секунд = 5 минут
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if check_vpn; then
            return 0
        fi
        log "VPN не доступен, попытка $attempt/$max_attempts. Ожидание 10 сек..."
        sleep 10
        ((attempt++))
    done

    return 1
}

# Основной скрипт
main() {
    log "=== Запуск отчета по привлечению ПВЗ ==="

    # Переходим в директорию со скриптом
    cd "$SCRIPT_DIR" || {
        log "Ошибка: не удалось перейти в $SCRIPT_DIR"
        exit 1
    }

    # Проверяем VPN
    log "Проверка VPN..."
    if ! check_vpn; then
        log "VPN не подключен. Ожидание подключения..."
        if ! wait_for_vpn; then
            log "Ошибка: VPN не подключился в течение 5 минут. Выход."
            exit 1
        fi
    fi
    log "VPN подключен."

    # Запускаем Python скрипт
    log "Запуск telegram.py..."
    "$PYTHON_PATH" "$SCRIPT_DIR/telegram.py" 2>&1 | tee -a "$LOG_FILE"

    local exit_code=${PIPESTATUS[0]}

    if [ $exit_code -eq 0 ]; then
        log "Отчет успешно отправлен."
    else
        log "Ошибка отправки отчета (код: $exit_code)"
    fi

    log "=== Завершение ==="
    echo "" >> "$LOG_FILE"

    exit $exit_code
}

main "$@"
