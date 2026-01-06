# PVZ Automation

Automated reports and monitoring for PVZ (pickup points).

## Features

- **Late Opening Reports** - Check if PVZ opened on time (09:00, 10:00 buckets)
- **Encashment Report** - Daily cash collection report
- **PVZ Landing Report** - Landing page statistics
- **VPN Monitor** - Auto-reconnect Netbird VPN with Telegram alerts

## Deployment

Runs on Raspberry Pi (denispi) via GitHub Actions self-hosted runner.

Push to `main` branch triggers automatic deployment.

## Schedule (MSK)

| Time | Task |
|------|------|
| 07:00 | Late Opening 09:00 |
| 08:00 | Late Opening 10:00 |
| 08:00 | PVZ Landing Report |
| 09:00 | Late Opening Final |
| 20:00 | Encashment Report |
| */5 min | VPN Monitor |

## Documentation

- [SCHEDULE.md](SCHEDULE.md) - Detailed schedule
- [RASPBERRY_SERVER.md](RASPBERRY_SERVER.md) - Server documentation
