#!/bin/bash
# PVZ Landing Report - запуск в 08:00 MSK
cd ~/pvz_automation
source venv/bin/activate
exec python3 pvz_landing/telegram.py >> ~/.pvz_landing_cron.log 2>&1
