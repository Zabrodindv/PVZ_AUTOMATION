"""
Конфигурация проекта pvz_automation.
Использует централизованные настройки из ~/.db_config
"""
import sys
from pathlib import Path

# Добавляем путь к централизованному конфигу
sys.path.insert(0, str(Path.home()))

# Реэкспортируем всё из централизованного конфига
from db_config import (
    DB_CONFIG,
    DB_DELIVERY_CONFIG,
    CH_CONFIG,
    CH_WMS_CONFIG,
    DATABASES,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    VPN_MONITOR_CHAT_ID,
    DELIVERY_POINT_CHAT_ID,
    get_pg_connection,
    get_ch_client,
    get_ch_wms_client,
)

__all__ = [
    "DB_CONFIG",
    "DB_DELIVERY_CONFIG",
    "CH_CONFIG",
    "CH_WMS_CONFIG",
    "DATABASES",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "VPN_MONITOR_CHAT_ID",
    "DELIVERY_POINT_CHAT_ID",
    "get_pg_connection",
    "get_ch_client",
    "get_ch_wms_client",
]
