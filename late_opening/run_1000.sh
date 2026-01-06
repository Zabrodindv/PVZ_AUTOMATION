#!/bin/bash
# Late Opening 10:00 bucket - запуск в 08:00 MSK
cd ~/pvz_automation
source venv/bin/activate
exec python3 late_opening/telegram.py --mode bucket --schedule 10:00 >> ~/.late_opening_cron.log 2>&1
