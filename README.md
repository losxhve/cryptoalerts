# CryptoAlerts - 加密货币行情监控告警系统

实时监控加密货币价格，达到设定条件时通过 Telegram 推送告警。

## 功能
- 监控 BTC/ETH/SOL 等主流币种价格
- 设置价格阈值告警（高于/低于）
- 通过 Telegram Bot 实时推送
- Web 管理面板

## 技术栈
- Backend: Python FastAPI
- Database: SQLite
- Price API: CoinGecko (免费)
- Alert: Telegram Bot
- Frontend: HTML + JS (内嵌)

## 快速启动

```bash
pip install -r requirements.txt
python main.py
```

打开 http://localhost:8000
