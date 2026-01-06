# VPN Monitor

Автоматическая система мониторинга и переподключения Netbird VPN.

## Описание

VPN Monitor проверяет состояние Netbird VPN каждый час и автоматически переподключается при обнаружении проблем. При всех событиях (отключение, переподключение) отправляются уведомления в Telegram.

## Возможности

- ✅ Автоматическая проверка каждый час через launchd
- ✅ Двухуровневая проверка подключения:
  - Статус netbird (`netbird status`)
  - Ping внутренних хостов (dwh-clickhouse, wms-clickhouse)
- ✅ Автоматическое переподключение через `netbird down` → `netbird up`
- ✅ До 3 попыток переподключения
- ✅ Telegram уведомления с защитой от спама (cooldown 30 минут)
- ✅ Сохранение состояния между запусками
- ✅ Ротация логов (max 10MB, 3 бэкапа)

## Установка

### 1. Файлы уже созданы в:

```
/Users/denis/db_connection_project/vpn_monitor/
├── monitor.py          # Основной скрипт мониторинга
├── run.sh              # Bash обёртка для launchd
└── README.md           # Эта документация

~/Library/LaunchAgents/com.vpn.monitor.plist  # Конфигурация launchd
```

### 2. Загрузить launchd job:

```bash
# Загрузить задачу
launchctl load ~/Library/LaunchAgents/com.vpn.monitor.plist

# Проверить статус
launchctl list | grep vpn.monitor
```

### 3. Готово!

Мониторинг запустится сразу и будет проверять VPN каждые 5 минут.

## Использование

### Просмотр логов

```bash
# Основной лог мониторинга (с ротацией)
tail -f ~/.vpn_monitor.log

# Лог wrapper скрипта
tail -f ~/.vpn_monitor_wrapper.log

# Вывод launchd
tail -f ~/.vpn_monitor_stdout.log
tail -f ~/.vpn_monitor_stderr.log
```

### Просмотр состояния

```bash
# Текущее состояние (JSON)
cat ~/.vpn_monitor_state.json
```

### Ручной запуск для тестирования

```bash
# Прямой запуск Python скрипта
cd /Users/denis/db_connection_project/vpn_monitor
python3 monitor.py

# Через wrapper (как launchd)
./run.sh
```

### Проверка работы launchd job

```bash
# Статус задачи
launchctl list | grep vpn.monitor

# Показать детали
launchctl print gui/$(id -u)/com.vpn.monitor

# Посмотреть когда запускался последний раз
log show --predicate 'process == "launchd"' --last 1h | grep vpn.monitor
```

## Управление

### Остановить мониторинг

```bash
launchctl unload ~/Library/LaunchAgents/com.vpn.monitor.plist
```

### Запустить снова

```bash
launchctl load ~/Library/LaunchAgents/com.vpn.monitor.plist
```

### Перезапустить с новыми настройками

```bash
launchctl unload ~/Library/LaunchAgents/com.vpn.monitor.plist
launchctl load ~/Library/LaunchAgents/com.vpn.monitor.plist
```

### Изменить частоту проверки

Отредактировать `~/Library/LaunchAgents/com.vpn.monitor.plist`:

```xml
<!-- Секунды: 3600 = 1 час, 1800 = 30 минут, 300 = 5 минут -->
<key>StartInterval</key>
<integer>3600</integer>
```

Затем перезагрузить:

```bash
launchctl unload ~/Library/LaunchAgents/com.vpn.monitor.plist
launchctl load ~/Library/LaunchAgents/com.vpn.monitor.plist
```

## Telegram уведомления

### Типы сообщений

1. **⚠️ VPN Отключение** - VPN не доступен, начинается переподключение
2. **✅ VPN Восстановлен** - Успешное переподключение
3. **❌ VPN Ошибка Переподключения** - Не удалось переподключить после 3 попыток
4. **✅ VPN Восстановлен Автоматически** - VPN восстановился сам (без нашего вмешательства)

### Защита от спама

- Уведомления об отключении отправляются максимум раз в 30 минут
- Уведомления об успешном переподключении отправляются всегда
- Ошибки переподключения отправляются всегда

## Конфигурация

### Telegram credentials

Используются из `.env` файла в корне проекта:

```env
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

### Внутренние хосты для проверки

Определены в `monitor.py`:

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
| `~/.vpn_monitor_wrapper.log` | Лог bash wrapper |
| `~/.vpn_monitor_stdout.log` | stdout от launchd |
| `~/.vpn_monitor_stderr.log` | stderr от launchd |

## Troubleshooting

### VPN не переподключается

1. Проверьте, что netbird установлен:
   ```bash
   which netbird
   netbird version
   ```

2. Попробуйте вручную:
   ```bash
   netbird down
   sleep 2
   netbird up
   ```

3. Проверьте логи:
   ```bash
   tail -100 ~/.vpn_monitor.log
   ```

### Не приходят уведомления в Telegram

1. Проверьте credentials в `.env`:
   ```bash
   cd /Users/denis/db_connection_project
   cat .env | grep TELEGRAM
   ```

2. Проверьте в логах наличие ошибок отправки:
   ```bash
   grep "Telegram" ~/.vpn_monitor.log
   ```

### launchd job не запускается

1. Проверьте права на файл:
   ```bash
   ls -la ~/Library/LaunchAgents/com.vpn.monitor.plist
   ```

2. Проверьте синтаксис plist:
   ```bash
   plutil ~/Library/LaunchAgents/com.vpn.monitor.plist
   ```

3. Посмотрите системный лог:
   ```bash
   log show --predicate 'subsystem == "com.apple.launchd"' --last 30m
   ```

### Очистка при необходимости

```bash
# Остановить мониторинг
launchctl unload ~/Library/LaunchAgents/com.vpn.monitor.plist

# Удалить файлы состояния и логов
rm ~/.vpn_monitor_state.json
rm ~/.vpn_monitor*.log

# Удалить всё
rm -rf /Users/denis/db_connection_project/vpn_monitor
rm ~/Library/LaunchAgents/com.vpn.monitor.plist
```

## Как это работает

1. **launchd** запускает `run.sh` каждый час (и сразу при загрузке)
2. **run.sh** активирует venv и запускает `monitor.py`
3. **monitor.py**:
   - Загружает состояние из JSON файла
   - Проверяет VPN через `netbird status` и ping
   - Если отключён - пытается переподключить (3 попытки)
   - Отправляет уведомления в Telegram
   - Сохраняет состояние
   - Пишет в лог

## Интеграция с другими задачами

Этот монитор работает независимо и помогает избежать проблем, когда другие задачи (например, encashment report) не могут подключиться к VPN.

Если VPN отключится в 20:00, а encashment job запустится в 22:00, то:
- VPN Monitor обнаружит отключение не позднее 21:00 (следующая проверка)
- Переподключит VPN в течение ~10 секунд
- К 22:00 VPN уже будет работать, и encashment job выполнится успешно
