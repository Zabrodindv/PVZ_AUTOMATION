#!/bin/bash
# Late Opening Final - запуск в 09:00 MSK
cd ~/pvz_automation
source venv/bin/activate
exec python3 late_opening/telegram.py --mode final >> ~/.late_opening_cron.log 2>&1
