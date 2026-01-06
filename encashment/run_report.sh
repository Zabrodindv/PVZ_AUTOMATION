#!/bin/bash
# Encashment Report - запуск в 20:00 MSK
cd ~/pvz_automation
source venv/bin/activate
exec python3 encashment/telegram.py >> ~/.encashment_cron.log 2>&1
