#!/bin/bash
# VPN Monitor - запуск каждые 5 минут
cd ~/pvz_automation
source venv/bin/activate
exec python3 vpn_monitor/monitor.py >> ~/.vpn_monitor_cron.log 2>&1
