# Быстрые команды

## Статус задач

```bash
# Все задачи
launchctl list | grep -E "(late.opening|encashment|pvz.landing|vpn.monitor)"

# Детальный статус конкретной задачи
launchctl print gui/$(id -u)/com.late.opening.1000
launchctl print gui/$(id -u)/com.late.opening.final
launchctl print gui/$(id -u)/com.encashment.report
```

## Файлы отслеживания

```bash
# Последний контрольный Late Opening
cat ~/.late_opening_last_run

# Последний Encashment
cat ~/.encashment_last_run
```

## Ручной запуск

### Late Opening

```bash
# Bucket 09:00
cd ~/pvz_automation/late_opening && ./run.sh --mode bucket --schedule 09:00

# Bucket 10:00
cd ~/pvz_automation/late_opening && ./run.sh --mode bucket --schedule 10:00

# Final (с защитой от повтора)
cd ~/pvz_automation/late_opening && ./run.sh --mode final

# Final принудительно
cd ~/pvz_automation/late_opening && ./run.sh --mode final --force
```

### Encashment

```bash
# Обычный запуск (с защитой от повтора)
cd ~/pvz_automation/encashment && ./run.sh

# Сбросить last_run и запустить заново
rm ~/.encashment_last_run && cd ~/pvz_automation/encashment && ./run.sh
```

### PVZ Landing

```bash
cd ~/pvz_automation/pvz_landing && ./run.sh
```

## Логи

```bash
# Late Opening
tail -50 ~/.late_opening_report.log
tail -f ~/.late_opening_report.log  # live

# Encashment
tail -50 ~/.encashment_report.log
tail -f ~/.encashment_report.log  # live

# PVZ Landing
tail -50 ~/.pvz_landing_report.log

# VPN Monitor
tail -50 ~/.vpn_monitor.log
```

## Управление launchd

```bash
# Перезагрузить задачу после изменения plist
launchctl unload ~/Library/LaunchAgents/com.late.opening.1000.plist
launchctl load ~/Library/LaunchAgents/com.late.opening.1000.plist

# Остановить задачу
launchctl unload ~/Library/LaunchAgents/com.encashment.report.plist

# Запустить задачу
launchctl load ~/Library/LaunchAgents/com.encashment.report.plist
```

## Проверка VPN

```bash
# Статус Netbird
netbird status

# Ping внутренних хостов
ping -c 1 dwh-clickhouse.prod.um.internal
ping -c 1 wms-clickhouse.prod.um.internal

# Переподключение
netbird down && netbird up
```

## Автопробуждение Mac

```bash
# Текущее расписание
pmset -g sched

# Настроить пробуждение в 06:55
sudo pmset repeat wake MTWRFSU 06:55:00

# Отменить автопробуждение
sudo pmset repeat cancel
```

## Сброс для повторной отправки

```bash
# Сбросить Late Opening Final
rm ~/.late_opening_last_run

# Сбросить Encashment
rm ~/.encashment_last_run
```
