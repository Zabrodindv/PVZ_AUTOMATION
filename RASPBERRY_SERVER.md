# Raspberry Pi Server - Документация

## Общая информация

| Параметр | Значение |
|----------|----------|
| Hostname | denispi |
| Локальный IP | 192.168.0.85 |
| Внешний IP | 188.134.93.57 |
| OS | Linux (Debian, aarch64) |
| VPN | Netbird |

---

## SSH подключение

### Из локальной сети
```bash
ssh denis@192.168.0.85
```

### Из интернета
```bash
ssh -p 2222 denis@188.134.93.57
```

### Проброс портов на роутере

| Внешний порт | Внутренний IP | Внутренний порт | Назначение |
|--------------|---------------|-----------------|------------|
| 2222 | 192.168.0.85 | 22 | SSH |
| 8443 | 192.168.0.85 | 8800 | Telegram Bot Webhook |

---

## VPN (Netbird)

### Статус
```bash
netbird status
netbird status -d  # детальный
```

### Управление
```bash
sudo netbird up      # подключить
sudo netbird down    # отключить
```

### Автозапуск
Netbird настроен как systemd сервис:
```bash
systemctl status netbird
systemctl is-enabled netbird  # должен быть enabled
```

### Авторизация
При первом запуске или истечении токена:
```bash
sudo netbird up
# Появится ссылка для авторизации через Google
# Откройте её в браузере и авторизуйтесь
```

### Внутренние хосты через VPN
- `wms-clickhouse.prod.um.internal` — ClickHouse WMS
- `dwh-clickhouse.prod.um.internal` — ClickHouse DWH

---

## Структура проекта

```
~/
├── db_config/              # Конфигурация БД (Python пакет)
│   ├── __init__.py
│   ├── config.py
│   ├── connections.py
│   └── .env                # Секреты (токены, пароли)
│
└── pvz_automation/         # Основной проект
    ├── .env -> ~/db_config/.env  # Симлинк
    ├── venv/               # Python виртуальное окружение
    ├── config.py           # Импорт из db_config
    ├── late_opening/       # Отчёт по открытию ПВЗ
    ├── encashment/         # Отчёт по инкассации
    ├── pvz_landing/        # Отчёт по лендингам
    └── vpn_monitor/        # Мониторинг VPN
```

---

## Запуск скриптов

### Активация окружения
```bash
cd ~/pvz_automation
source venv/bin/activate
```

### Отчёт по открытию ПВЗ (без отправки в Telegram)
```bash
python3 late_opening/report.py
```

### Отчёт по открытию ПВЗ (с отправкой в Telegram)
```bash
python3 late_opening/telegram.py --mode final
python3 late_opening/telegram.py --mode bucket --schedule 09:00
```

### VPN Monitor (ручной запуск)
```bash
python3 vpn_monitor/monitor.py
```

---

## Автоматизация (Cron)

### Текущие задачи
```bash
crontab -l
```

### Расписание (МСК)

| Время | Задача | Скрипт |
|-------|--------|--------|
| */5 * * * * | VPN Monitor | `vpn_monitor/run_monitor.sh` |
| 0 7 * * * | Late Opening 09:00 | `late_opening/run_0900.sh` |
| 0 8 * * * | Late Opening 10:00 | `late_opening/run_1000.sh` |
| 0 8 * * * | PVZ Landing | `pvz_landing/run_report.sh` |
| 0 9 * * * | Late Opening Final | `late_opening/run_final.sh` |
| 0 20 * * * | Encashment | `encashment/run_report.sh` |

### Редактирование cron
```bash
crontab -e
```

---

## Мониторинг VPN

### Логика работы
1. Каждые 5 минут запускается `vpn_monitor/monitor.py`
2. Проверяется:
   - Статус Netbird (`netbird status`)
   - Ping внутренних хостов через VPN
3. При отключении:
   - Отправляется уведомление в Telegram
   - Автоматическая попытка переподключения (3 попытки)
   - Уведомление об успехе/неудаче

### Уведомления в Telegram
- **VPN алерты** → личный чат (862779466)
- **Отчёты ПВЗ** → групповой чат (-1003514702147)

### Логи
```bash
# Подробный лог мониторинга
tail -f ~/.vpn_monitor.log

# Лог cron
tail -f ~/.vpn_monitor_cron.log

# Состояние мониторинга
cat ~/.vpn_monitor_state.json
```

---

## Базы данных

### ClickHouse WMS
- Host: `wms-clickhouse.prod.um.internal`
- Доступ через VPN

### PostgreSQL (Yandex Cloud)
- Host: `c-c9q31ea29f7jk266tat3.ro.mdb.yandexcloud.net`
- Database: `delivery-point`

### Тестирование подключения
```bash
cd ~/pvz_automation
source venv/bin/activate
python3 ~/test_db.py
```

---

## Устранение неполадок

### VPN не подключается
```bash
# Проверить статус
netbird status

# Попробовать переподключить
sudo netbird down
sudo netbird up

# Проверить логи
journalctl -u netbird -f
```

### Telegram не отправляет сообщения
Проблема с IPv6. Решение уже применено (`/etc/gai.conf`).

Проверка:
```bash
curl -v https://api.telegram.org
# Должен подключаться по IPv4
```

### Скрипты не работают
```bash
# Проверить виртуальное окружение
cd ~/pvz_automation
source venv/bin/activate
which python3  # должен быть ~/pvz_automation/venv/bin/python3

# Проверить .env
ls -la .env
cat .env | head -5
```

### SSH не подключается извне
1. Проверить внешний IP: https://2ip.ru
2. Проверить проброс порта на роутере
3. Проверить SSH на Raspberry: `sudo systemctl status ssh`

---

## Полезные команды

```bash
# Перезагрузка
sudo reboot

# Свободное место
df -h

# Память
free -h

# Процессы Python
ps aux | grep python

# Сетевые подключения
ss -tulpn

# Внешний IP
curl ifconfig.me
```

---

## Обновление проекта

С Mac:
```bash
scp -P 2222 -r /Users/denis/pvz_automation denis@188.134.93.57:~/
scp -P 2222 -r /Users/denis/db_config denis@188.134.93.57:~/
```

На Raspberry после обновления:
```bash
cd ~/pvz_automation
source venv/bin/activate
pip install -r requirements.txt  # если добавились зависимости
```
