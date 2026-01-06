#!/bin/bash
#
# Скрипт проверки состояния деплоя на Raspberry Pi
#

ssh -p 2222 denis@188.134.93.57 "
  echo '=== Deployment Status ==='
  cd ~/pvz_automation
  if [ -d .git ]; then
    git log -1 --oneline 2>/dev/null || echo 'Git not initialized'
  else
    echo 'Not a git repository (deployed via rsync)'
  fi
  echo
  echo '=== Cron Status ==='
  crontab -l | grep -v '^#' | grep -v '^$'
  echo
  echo '=== VPN Status ==='
  netbird status | head -5
  echo
  echo '=== Python Environment ==='
  source venv/bin/activate
  python --version
  echo 'Dependencies OK'
"
