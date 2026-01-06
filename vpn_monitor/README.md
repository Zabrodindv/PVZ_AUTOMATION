# VPN Monitor

Автоматическая система мониторинга и переподключения Netbird VPN на Raspberry Pi.

## Описание

VPN Monitor проверяет состояние Netbird VPN каждые 5 минут и автоматически переподключается при обнаружении проблем. Уведомления отправляются в личный Telegram чат.

## Возможности

- ✅ Автоматическая проверка каждые 5 минут через cron
- ✅ Двухуровневая проверка подключения:
  - Статус netbird (`netbird status`)
  - Ping внутренних хостов (dwh-clickhouse, wms-clickhouse)
- ✅ Автоматическое переподключение через `netbird down` → `netbird up`
- ✅ До 3 попыток переподключения
- ✅ Telegram уведомления с защитой от спама (cooldown 30 минут)
- ✅ Сохранение состояния между запусками
- ✅ Ротация логов (max 10MB, 3 бэкапа)

## Структура файлов

```
~/pvz_automation/vpn_monitor/
├── monitor.py          # Основной скрипт мониторинга
├── run_monitor.sh      # Bash обёртка для cron
├── run.sh              # Legacy wrapper (не используется)
└── README.md           # Эта документация
```

## Cron конфигурация

```cron
# Каждые 5 минут
*/5 * * * * /home/denis/pvz_automation/vpn_monitor/run_monitor.sh
```

Проверить/изменить:
```bash
crontab -e
crontab -l
```

## Использование

### Просмотр логов

```bash
# Основной лог мониторинга (с ротацией)
tail -f ~/.vpn_monitor.log

# Лог cron wrapper
tail -f ~/.vpn_monitor_cron.log
```

### Просмотр состояния

```bash
# Текущее состояние (JSON)
cat ~/.vpn_monitor_state.json
```

Пример состояния:
```json
{
  "last_check": "2026-01-06T09:05:00.123456",
  "last_status": "connected",
  "last_notification_time": null,
  "reconnect_count": 0,
  "consecutive_failures": 0
}
```

### Ручной запуск для тестирования

```bash
cd ~/pvz_automation
source venv/bin/activate
python3 vpn_monitor/monitor.py
```

## Telegram уведомления

### Куда отправляются

- **Chat ID**: `862779466` (личный чат)
- Настраивается через `VPN_MONITOR_CHAT_ID` в `.env`

### Типы сообщений

| Emoji | Событие | Описание |
|-------|---------|----------|
| ⚠️ | VPN Отключение | VPN не доступен, начинается переподключение |
| ✅ | VPN Восстановлен | Успешное переподключение (с номером попытки) |
| ❌ | VPN Ошибка | Не удалось переподключить после 3 попыток |
| ✅ | Автовосстановление | VPN восстановился сам |

### Защита от спама

- Уведомления об отключении: максимум раз в 30 минут
- Уведомления об успехе/ошибке: отправляются всегда

## Конфигурация

### Telegram credentials

В `~/db_config/.env` (симлинк `~/pvz_automation/.env`):

```env
TELEGRAM_BOT_TOKEN=8009409268:AAF...
VPN_MONITOR_CHAT_ID=862779466
```

### Внутренние хосты для проверки

В `monitor.py`:

```python
VPN_HOSTS = [
    "wms-clickhouse.prod.um.internal",
    "dwh-clickhouse.prod.um.internal",
]
```

## Файлы состояния и логов

| Файл | Описание |
|------|----------|
| `~/.vpn_monitor_state.json` | Состояние (последняя проверка, статус, счётчики) |
| `~/.vpn_monitor.log` | Основной лог с ротацией (10MB × 3) |
| `~/.vpn_monitor_cron.log` | Лог cron wrapper |

## Логика работы

```
┌─────────────────────────────────────────────────────────┐
│                    Cron (каждые 5 мин)                  │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│              1. Загрузить состояние из JSON             │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│              2. Проверить VPN                           │
│                 - netbird status                        │
│                 - ping внутренних хостов                │
└─────────────────────────────────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
        VPN OK                    VPN DOWN
              │                         │
              ▼                         ▼
┌─────────────────────┐   ┌─────────────────────────────┐
│ Был disconnected?   │   │ Отправить ⚠️ (с cooldown)   │
│ → Отправить ✅      │   │ Переподключить (3 попытки) │
│   "автовосстановл." │   │ → Успех: отправить ✅       │
└─────────────────────┘   │ → Ошибка: отправить ❌      │
              │           └─────────────────────────────┘
              │                         │
              └────────────┬────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│              3. Сохранить состояние в JSON              │
└─────────────────────────────────────────────────────────┘
```

## Troubleshooting

### VPN не переподключается

```bash
# Проверить netbird
netbird status
netbird status -d

# Вручную переподключить
sudo netbird down
sleep 2
sudo netbird up

# Проверить логи
tail -50 ~/.vpn_monitor.log
```

### Не приходят уведомления

```bash
# Проверить credentials
cat ~/db_config/.env | grep -E "(TELEGRAM|VPN_MONITOR)"

# Проверить в логах
grep "Telegram" ~/.vpn_monitor.log | tail -10

# Тест отправки
curl -s "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/sendMessage" \
  -d "chat_id=862779466" \
  -d "text=Test from Raspberry Pi"
```

### Cron не запускается

```bash
# Проверить cron
crontab -l | grep vpn

# Проверить права
ls -la ~/pvz_automation/vpn_monitor/run_monitor.sh

# Проверить syslog
grep CRON /var/log/syslog | tail -20
```

## Интеграция с другими задачами

VPN Monitor работает независимо и обеспечивает стабильность VPN для других отчётов:

| Время | Задача | VPN нужен? |
|-------|--------|------------|
| */5 min | VPN Monitor | Проверяет |
| 07:00 | Late Opening 0900 | ✅ |
| 08:00 | Late Opening 1000 | ✅ |
| 08:00 | PVZ Landing | ✅ |
| 09:00 | Late Opening Final | ✅ |
| 20:00 | Encashment | ✅ |

Если VPN отключится, монитор обнаружит это максимум через 5 минут и переподключит.
