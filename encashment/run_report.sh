#!/bin/bash
# Encashment Report - запуск в 10:00 Ташкент (08:00 МСК) за предыдущий день
cd ~/pvz_automation
source venv/bin/activate
exec python3 encashment/telegram.py >> ~/.encashment_cron.log 2>&1
