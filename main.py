"""
加密货币行情监控告警系统
CryptoAlerts - 实时监控 + Telegram 推送
"""

import sqlite3
import json
import httpx
import asyncio
import hashlib
import os
import secrets
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ========== 配置 ==========
DB_PATH = "data/cryptoalerts.db"
COINGECKO_API = "https://api.coingecko.com/api/v3"
CHECK_INTERVAL = 60  # 秒
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

# ========== 定价 ==========
PLANS = {
    "free": {"alerts": 5, "interval": 300, "price_usd": 0},
    "pro":  {"alerts": 100, "interval": 60, "price_usd": 9.99},
    "max":  {"alerts": 999, "interval": 15, "price_usd": 29.99},
}
# 收款钱包地址
PAYMENT_WALLET = os.getenv("PAYMENT_WALLET", "TBzeBcdaEGvS2FLnW7wjHuN19RqUaVpFHH")
PAYMENT_CHAIN = os.getenv("PAYMENT_CHAIN", "TRC-20 (USDT)")
# 用于验证付款的OKX API（可选）
OKX_API_KEY = os.getenv("OKX_API_KEY", "")

# ========== 数据库 ==========
def init_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE,
            password_hash TEXT,
            token TEXT,
            telegram_chat_id TEXT,
            plan TEXT DEFAULT 'free',
            plan_expires TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            coin_id TEXT NOT NULL,
            coin_symbol TEXT NOT NULL,
            alert_type TEXT NOT NULL CHECK(alert_type IN ('above','below')),
            target_price REAL NOT NULL,
            is_active INTEGER DEFAULT 1,
            last_triggered TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS price_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coin_id TEXT NOT NULL,
            price REAL NOT NULL,
            timestamp TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            tx_hash TEXT,
            amount_usd REAL NOT NULL,
            plan TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
    """)
    conn.commit()
    conn.close()

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ========== CoinGecko 价格获取 ==========
SUPPORTED_COINS = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "ripple": "XRP",
    "dogecoin": "DOGE",
    "cardano": "ADA",
    "polkadot": "DOT",
    "avalanche-2": "AVAX",
    "chainlink": "LINK",
    "polygon": "MATIC",
    "tron": "TRX",
    "litecoin": "LTC",
    "arbitrum": "ARB",
    "optimism": "OP",
    "sui": "SUI",
}

async def fetch_prices():
    """获取所有支持币种的最新价格"""
    ids = ",".join(SUPPORTED_COINS.keys())
    url = f"{COINGECKO_API}/simple/price?ids={ids}&vs_currencies=usd"
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(url)
            data = resp.json()
            prices = {}
            for coin_id, sym in SUPPORTED_COINS.items():
                if coin_id in data and "usd" in data[coin_id]:
                    prices[coin_id] = {"symbol": sym, "price": data[coin_id]["usd"]}
            return prices
        except Exception as e:
            print(f"[Error] Fetch prices: {e}")
            return {}

# ========== 告警检查 ==========
async def check_alerts():
    """定期检查所有活跃告警"""
    prices = await fetch_prices()
    if not prices:
        return
    
    # 记录价格历史
    conn = get_db()
    for coin_id, info in prices.items():
        conn.execute(
            "INSERT INTO price_history (coin_id, price) VALUES (?, ?)",
            (coin_id, info["price"])
        )
    conn.commit()
    
    # 检查告警
    alerts = conn.execute(
        "SELECT a.*, u.telegram_chat_id FROM alerts a JOIN users u ON a.user_id = u.id "
        "WHERE a.is_active = 1"
    ).fetchall()
    
    for alert in alerts:
        coin_id = alert["coin_id"]
        if coin_id not in prices:
            continue
        
        current_price = prices[coin_id]["price"]
        triggered = False
        
        if alert["alert_type"] == "above" and current_price >= alert["target_price"]:
            triggered = True
        elif alert["alert_type"] == "below" and current_price <= alert["target_price"]:
            triggered = True
        
        if triggered:
            msg = (
                f"🚨 *CryptoAlert 告警*\n"
                f"{prices[coin_id]['symbol']} ({coin_id.capitalize()})\n"
                f"当前价格: ${current_price:,.4f}\n"
                f"触发条件: {alert['alert_type']} ${alert['target_price']:,.4f}"
            )
            await send_telegram(alert["telegram_chat_id"], msg) if alert["telegram_chat_id"] else None
            
            conn.execute(
                "UPDATE alerts SET last_triggered = datetime('now') WHERE id = ?",
                (alert["id"],)
            )
    
    conn.commit()
    conn.close()

# ========== Telegram 推送 ==========
async def send_telegram(chat_id: str, message: str):
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            await client.post(url, json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "Markdown"
            })
            return True
        except:
            return False

# ========== 调度器 ==========
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler.add_job(check_alerts, "interval", seconds=CHECK_INTERVAL, id="check_prices")
    scheduler.start()
    yield
    scheduler.shutdown()

# ========== FastAPI App ==========
app = FastAPI(title="CryptoAlerts", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ========== API 路由 ==========
@app.get("/api/prices")
async def get_prices():
    """获取实时价格"""
    prices = await fetch_prices()
    return prices

@app.get("/api/coins")
async def get_coins():
    """获取支持的币种列表"""
    return [{"id": k, "symbol": v} for k, v in SUPPORTED_COINS.items()]

# ========== 认证 ==========
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def generate_token() -> str:
    return secrets.token_hex(32)

@app.post("/api/auth/register")
async def auth_register(request: Request):
    """邮箱+密码注册"""
    data = await request.json()
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")
    username = data.get("username", email.split("@")[0])
    
    if not email or "@" not in email:
        raise HTTPException(400, "请输入有效邮箱")
    if len(password) < 6:
        raise HTTPException(400, "密码至少6位")
    
    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        conn.close()
        raise HTTPException(400, "该邮箱已注册")
    
    token = generate_token()
    conn.execute(
        "INSERT INTO users (username, email, password_hash, token) VALUES (?, ?, ?, ?)",
        (username, email, hash_password(password), token)
    )
    conn.commit()
    user_id = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()[0]
    conn.close()
    return {"user_id": user_id, "username": username, "email": email, "token": token, "plan": "free"}

@app.post("/api/auth/login")
async def auth_login(request: Request):
    """邮箱+密码登录"""
    data = await request.json()
    email = data.get("email", "").strip().lower()
    password = data.get("password", "")
    
    conn = get_db()
    user = conn.execute(
        "SELECT id, username, email, password_hash, plan, plan_expires FROM users WHERE email = ?",
        (email,)
    ).fetchone()
    
    if not user or user["password_hash"] != hash_password(password):
        conn.close()
        raise HTTPException(401, "邮箱或密码错误")
    
    token = generate_token()
    conn.execute("UPDATE users SET token = ? WHERE id = ?", (token, user["id"]))
    conn.commit()
    conn.close()
    return {
        "user_id": user["id"], "username": user["username"],
        "email": user["email"], "token": token, "plan": user["plan"]
    }

@app.post("/api/auth/check")
async def auth_check(request: Request):
    """验证token是否有效"""
    data = await request.json()
    token = data.get("token", "")
    if not token:
        raise HTTPException(401, "未登录")
    conn = get_db()
    user = conn.execute(
        "SELECT id, username, email, plan FROM users WHERE token = ?", (token,)
    ).fetchone()
    conn.close()
    if not user:
        raise HTTPException(401, "登录已过期")
    return dict(user)

@app.post("/api/register")
async def register(request: Request):
    """用户注册/登录（兼容旧版无密码模式）"""
    data = await request.json()
    username = data.get("username")
    if not username:
        raise HTTPException(400, "username required")
    
    conn = get_db()
    # 检查用户是否存在 → 登录
    existing = conn.execute("SELECT id, plan, plan_expires FROM users WHERE username = ?", (username,)).fetchone()
    if existing:
        conn.close()
        return {"user_id": existing["id"], "username": username, "plan": existing["plan"], "is_new": False}
    
    # 不存在 → 注册新用户
    try:
        conn.execute("INSERT INTO users (username) VALUES (?)", (username,))
        conn.commit()
        user_id = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()[0]
        conn.close()
        return {"user_id": user_id, "username": username, "plan": "free", "is_new": True}
    except sqlite3.IntegrityError:
        conn.close()
        raise HTTPException(400, "username already exists")

@app.post("/api/alerts")
async def create_alert(request: Request):
    """创建告警"""
    data = await request.json()
    required = ["user_id", "coin_id", "alert_type", "target_price"]
    for field in required:
        if field not in data:
            raise HTTPException(400, f"{field} required")
    
    if data["coin_id"] not in SUPPORTED_COINS:
        raise HTTPException(400, f"unsupported coin: {data['coin_id']}")
    if data["alert_type"] not in ("above", "below"):
        raise HTTPException(400, "alert_type must be 'above' or 'below'")
    
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (data["user_id"],)).fetchone()
    if not user:
        conn.close()
        raise HTTPException(404, "user not found")
    
    # 检查配额
    plan = user["plan"]
    alert_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM alerts WHERE user_id = ? AND is_active = 1",
        (data["user_id"],)
    ).fetchone()["cnt"]
    
    max_alerts = PLANS.get(plan, PLANS["free"])["alerts"]
    if alert_count >= max_alerts:
        conn.close()
        raise HTTPException(402, f"已达上限({max_alerts}个)，请升级套餐")
    
    conn.execute(
        "INSERT INTO alerts (user_id, coin_id, coin_symbol, alert_type, target_price) VALUES (?, ?, ?, ?, ?)",
        (data["user_id"], data["coin_id"], SUPPORTED_COINS[data["coin_id"]], data["alert_type"], data["target_price"])
    )
    conn.commit()
    alert_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return {"alert_id": alert_id}

@app.get("/api/alerts/{user_id}")
async def get_alerts(user_id: int):
    """获取用户的告警列表"""
    conn = get_db()
    alerts = conn.execute(
        "SELECT * FROM alerts WHERE user_id = ? ORDER BY created_at DESC", (user_id,)
    ).fetchall()
    conn.close()
    return [dict(a) for a in alerts]

@app.delete("/api/alerts/{alert_id}")
async def delete_alert(alert_id: int):
    """删除告警"""
    conn = get_db()
    conn.execute("DELETE FROM alerts WHERE id = ?", (alert_id,))
    conn.commit()
    conn.close()
    return {"status": "deleted"}

@app.post("/api/telegram/bind")
async def bind_telegram(request: Request):
    """绑定 Telegram 机器人"""
    data = await request.json()
    user_id = data.get("user_id")
    chat_id = data.get("chat_id")
    if not user_id or not chat_id:
        raise HTTPException(400, "user_id and chat_id required")
    conn = get_db()
    conn.execute("UPDATE users SET telegram_chat_id = ? WHERE id = ?", (chat_id, user_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

@app.get("/api/prices/history/{coin_id}")
async def get_price_history(coin_id: str):
    """获取价格历史"""
    conn = get_db()
    rows = conn.execute(
        "SELECT price, timestamp FROM price_history WHERE coin_id = ? ORDER BY timestamp DESC LIMIT 100",
        (coin_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ========== 支付 & 订阅 ==========
@app.get("/api/plans")
async def get_plans():
    """获取套餐信息"""
    return {
        "wallet": PAYMENT_WALLET,
        "chain": PAYMENT_CHAIN,
        "plans": {
            "free": {"alerts": 5, "interval_sec": 300, "price_usd": 0},
            "pro":  {"alerts": 100, "interval_sec": 60, "price_usd": 9.99},
            "max":  {"alerts": 999, "interval_sec": 15, "price_usd": 29.99},
        }
    }

@app.post("/api/payment/verify")
async def verify_payment(request: Request):
    """验证用户是否已付款（手动确认模式）"""
    data = await request.json()
    user_id = data.get("user_id")
    plan = data.get("plan", "pro")
    tx_hash = data.get("tx_hash", "")
    
    if not user_id or plan not in PLANS:
        raise HTTPException(400, "invalid request")
    
    price = PLANS[plan]["price_usd"]
    if price == 0:
        raise HTTPException(400, "free plan doesn't need payment")
    
    conn = get_db()
    conn.execute(
        "INSERT INTO payments (user_id, plan, amount_usd, tx_hash, status) VALUES (?, ?, ?, ?, 'confirmed')",
        (user_id, plan, price, tx_hash)
    )
    conn.execute(
        "UPDATE users SET plan = ?, plan_expires = datetime('now', '+30 days') WHERE id = ?",
        (plan, user_id)
    )
    conn.commit()
    conn.close()
    return {"status": "upgraded", "plan": plan}

@app.get("/api/user/{user_id}/usage")
async def get_usage(user_id: int):
    """获取用户使用情况和配额"""
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    if not user:
        conn.close()
        raise HTTPException(404, "user not found")
    
    plan = user["plan"]
    alert_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM alerts WHERE user_id = ? AND is_active = 1",
        (user_id,)
    ).fetchone()["cnt"]
    
    conn.close()
    return {
        "plan": plan,
        "plan_expires": user["plan_expires"],
        "alerts_used": alert_count,
        "alerts_max": PLANS[plan]["alerts"],
        "check_interval_sec": PLANS[plan]["interval"],
    }

@app.get("/", response_class=HTMLResponse)
async def index():
    """Web 管理面板"""
    return HTMLResponse(INDEX_HTML)

# ========== 前端页面 ==========
INDEX_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CryptoAlerts - 加密货币行情监控</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }
        header { text-align: center; padding: 40px 0 30px; }
        header h1 { font-size: 36px; background: linear-gradient(135deg, #f59e0b, #3b82f6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        header p { color: #94a3b8; margin-top: 8px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 16px; margin: 24px 0; }
        .coin-card { background: #1e293b; border-radius: 12px; padding: 20px; border: 1px solid #334155; transition: all 0.2s; }
        .coin-card:hover { border-color: #3b82f6; transform: translateY(-2px); }
        .coin-card .symbol { font-size: 24px; font-weight: 700; color: #f1f5f9; }
        .coin-card .price { font-size: 20px; color: #22c55e; margin-top: 8px; font-weight: 600; }
        .coin-card .change { font-size: 14px; color: #94a3b8; margin-top: 4px; }
        .section { margin: 32px 0; }
        .section h2 { font-size: 22px; margin-bottom: 16px; color: #f1f5f9; }
        .form-group { display: flex; gap: 12px; flex-wrap: wrap; align-items: end; margin: 16px 0; }
        .form-group label { font-size: 14px; color: #94a3b8; display: block; margin-bottom: 4px; }
        select, input { padding: 10px 14px; border-radius: 8px; border: 1px solid #475569; background: #0f172a; color: #e2e8f0; font-size: 14px; outline: none; }
        select:focus, input:focus { border-color: #3b82f6; }
        button { padding: 10px 24px; border-radius: 8px; border: none; font-size: 14px; font-weight: 600; cursor: pointer; transition: all 0.2s; }
        .btn-primary { background: #3b82f6; color: white; }
        .btn-primary:hover { background: #2563eb; }
        .btn-danger { background: #ef4444; color: white; padding: 6px 14px; font-size: 12px; }
        .btn-danger:hover { background: #dc2626; }
        table { width: 100%; border-collapse: collapse; margin: 16px 0; }
        th, td { text-align: left; padding: 12px 16px; border-bottom: 1px solid #334155; font-size: 14px; }
        th { color: #94a3b8; font-weight: 500; }
        .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 600; }
        .badge-active { background: #22c55e20; color: #22c55e; }
        .badge-above { background: #3b82f620; color: #3b82f6; }
        .badge-below { background: #f59e0b20; color: #f59e0b; }
        .toast { position: fixed; bottom: 20px; right: 20px; padding: 14px 24px; border-radius: 8px; color: white; font-weight: 500; z-index: 100; animation: slideIn 0.3s; }
        .toast-success { background: #22c55e; }
        .toast-error { background: #ef4444; }
        @keyframes slideIn { from { transform: translateX(100px); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
        .empty { text-align: center; color: #64748b; padding: 40px; font-size: 16px; }
        .user-setup { background: #1e293b; border-radius: 12px; padding: 24px; border: 1px solid #334155; margin: 16px 0; }
        .telegram-guide { font-size: 14px; color: #94a3b8; margin: 8px 0; line-height: 1.6; }
        code { background: #0f172a; padding: 2px 6px; border-radius: 4px; color: #a5b4fc; }
        .loading { text-align: center; padding: 60px; color: #64748b; }
        .spinner { display: inline-block; width: 40px; height: 40px; border: 3px solid #334155; border-top-color: #3b82f6; border-radius: 50%; animation: spin 0.8s linear infinite; }
        @keyframes spin { to { transform: rotate(360deg); } }
    </style>
</head>
<body>
    <div class="container" id="app">
        <header>
            <h1>🚨 CryptoAlerts</h1>
            <p>实时加密货币监控 · Telegram 告警推送</p>
        </header>

        <div class="user-setup" id="authSection">
            <div style="display:flex;gap:16px;align-items:center;margin-bottom:16px;">
                <h2 style="margin:0;">🔐 登录</h2>
                <span id="authToggle" style="color:#3b82f6;cursor:pointer;font-size:14px;" onclick="toggleAuth()">没有账号？去注册</span>
            </div>
            <div id="authForm">
                <div class="form-group">
                    <div style="flex:1;">
                        <label>邮箱</label>
                        <input type="email" id="authEmail" style="width:100%;" placeholder="your@email.com">
                    </div>
                    <div style="flex:1;">
                        <label>密码</label>
                        <input type="password" id="authPassword" style="width:100%;" placeholder="至少6位">
                    </div>
                    <button class="btn-primary" id="authBtn" onclick="doLogin()">登录</button>
                </div>
            </div>
            <div id="userInfo" style="display:none;">
                <div style="display:flex;justify-content:space-between;align-items:center;">
                    <div>
                        <span style="font-size:18px;font-weight:600;" id="displayEmail"></span>
                        <span style="font-size:14px;color:#94a3b8;margin-left:8px;" id="displayPlan"></span>
                    </div>
                    <button style="background:#475569;color:white;border:none;padding:6px 16px;border-radius:6px;cursor:pointer;font-size:13px;" onclick="logout()">退出</button>
                </div>
            </div>
            <div class="telegram-guide" style="margin-top:12px;">
                📱 绑定 Telegram：搜索 <code>@CryptoAlertsBot</code> → 发送 <code>/start</code> → 输入你的邮箱完成绑定
            </div>
        </div>

        <div class="section">
            <h2>📊 实时行情</h2>
            <div class="grid" id="priceGrid">
                <div class="loading"><div class="spinner"></div><p>加载中...</p></div>
            </div>
        </div>

        <div class="section">
            <h2>➕ 创建告警</h2>
            <div class="form-group">
                <div>
                    <label>币种</label>
                    <select id="coinSelect"></select>
                </div>
                <div>
                    <label>条件</label>
                    <select id="alertType">
                        <option value="above">价格高于 (Above)</option>
                        <option value="below">价格低于 (Below)</option>
                    </select>
                </div>
                <div>
                    <label>目标价格 (USD)</label>
                    <input type="number" id="targetPrice" step="0.0001" placeholder="100000">
                </div>
                <button class="btn-primary" onclick="createAlert()">创建告警</button>
            </div>
        </div>

        <div class="section">
            <h2>💎 升级套餐</h2>
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:16px;">
                <div class="coin-card" style="text-align:center;">
                    <h3 style="color:#94a3b8;">免费版</h3>
                    <div style="font-size:36px;font-weight:700;color:#f1f5f9;margin:12px 0;">$0</div>
                    <p style="color:#94a3b8;font-size:14px;">5个告警 / 5分钟检查</p>
                    <p style="color:#22c55e;font-size:12px;margin-top:8px;">当前使用中</p>
                </div>
                <div class="coin-card" style="text-align:center;border-color:#3b82f6;">
                    <h3 style="color:#3b82f6;">Pro</h3>
                    <div style="font-size:36px;font-weight:700;color:#f1f5f9;margin:12px 0;">$9.99</div>
                    <p style="color:#94a3b8;font-size:14px;">100个告警 / 1分钟检查</p>
                    <p style="color:#94a3b8;font-size:14px;">Telegram推送</p>
                    <button class="btn-primary" style="margin-top:12px;width:100%;" onclick="upgrade('pro')">升级Pro</button>
                </div>
                <div class="coin-card" style="text-align:center;border-color:#f59e0b;">
                    <h3 style="color:#f59e0b;">Max</h3>
                    <div style="font-size:36px;font-weight:700;color:#f1f5f9;margin:12px 0;">$29.99</div>
                    <p style="color:#94a3b8;font-size:14px;">999个告警 / 15秒检查</p>
                    <p style="color:#94a3b8;font-size:14px;">实时推送+优先支持</p>
                    <button class="btn-primary" style="margin-top:12px;width:100%;background:#f59e0b;" onclick="upgrade('max')">升级Max</button>
                </div>
            </div>
        </div>

        <div class="section" id="paymentSection" style="display:none;">
            <h2>💳 支付</h2>
            <div class="user-setup" id="paymentInfo"></div>
        </div>

        <div class="section">
            <h2>📋 我的告警</h2>
            <table>
                <thead><tr><th>币种</th><th>条件</th><th>目标价</th><th>当前价</th><th>状态</th><th>操作</th></tr></thead>
                <tbody id="alertTable"><tr><td colspan="6" class="empty">请先注册用户</td></tr></tbody>
            </table>
        </div>
    </div>

    <script>
        let currentUserId = null;
        let currentToken = localStorage.getItem('crypto_token') || '';
        let prices = {};
        let isLoginMode = true;

        function toggleAuth() {
            isLoginMode = !isLoginMode;
            document.getElementById('authBtn').textContent = isLoginMode ? '登录' : '注册';
            document.getElementById('authToggle').textContent = isLoginMode ? '没有账号？去注册' : '已有账号？去登录';
        }

        async function doLogin() {
            const email = document.getElementById('authEmail').value.trim();
            const password = document.getElementById('authPassword').value.trim();
            if (!email || !password) return showToast('请填写邮箱和密码', 'error');
            
            const endpoint = isLoginMode ? '/api/auth/login' : '/api/auth/register';
            const res = await api(endpoint, { method: 'POST', body: JSON.stringify({email, password}) });
            if (res && res.token) {
                currentUserId = res.user_id;
                currentToken = res.token;
                localStorage.setItem('crypto_token', res.token);
                localStorage.setItem('crypto_email', email);
                showAuthSuccess(res);
                showToast('登录成功 🎉');
                loadAlerts();
            }
        }

        async function checkAuth() {
            if (!currentToken) return;
            const res = await api('/api/auth/check', { method: 'POST', body: JSON.stringify({token: currentToken}) });
            if (res && res.id) {
                currentUserId = res.id;
                showAuthSuccess({...res, email: res.email || localStorage.getItem('crypto_email')});
                loadAlerts();
            } else {
                localStorage.removeItem('crypto_token');
            }
        }

        function showAuthSuccess(res) {
            document.getElementById('authForm').style.display = 'none';
            document.getElementById('authToggle').style.display = 'none';
            document.getElementById('userInfo').style.display = 'block';
            document.getElementById('displayEmail').textContent = res.email || '已登录';
            document.getElementById('displayPlan').textContent = `(${res.plan || 'free'})`;
        }

        function logout() {
            currentToken = '';
            currentUserId = null;
            localStorage.removeItem('crypto_token');
            localStorage.removeItem('crypto_email');
            document.getElementById('authForm').style.display = 'block';
            document.getElementById('authToggle').style.display = 'inline';
            document.getElementById('userInfo').style.display = 'none';
            document.getElementById('alertTable').innerHTML = '<tr><td colspan="6" class="empty">请先登录</td></tr>';
            showToast('已退出');
        }

        async function api(url, opts = {}) {
            try {
                const resp = await fetch(url, { headers: {'Content-Type': 'application/json'}, ...opts });
                const data = await resp.json();
                if (!resp.ok) {
                    showToast(data.detail || '请求失败', 'error');
                    return null;
                }
                return data;
            } catch(e) { showToast('网络错误: ' + e.message, 'error'); return null; }
        }

        function showToast(msg, type = 'success') {
            const t = document.createElement('div');
            t.className = 'toast toast-' + type;
            t.textContent = msg;
            document.body.appendChild(t);
            setTimeout(() => t.remove(), 3000);
        }

        async function register() {
            const username = document.getElementById('username').value.trim();
            if (!username) return showToast('请输入用户名', 'error');
            const res = await api('/api/register', { method: 'POST', body: JSON.stringify({username}) });
            if (res) {
                currentUserId = res.user_id;
                document.getElementById('userStatus').textContent = '✅ 已登录: ' + username;
                loadAlerts();
                showToast('登录成功', 'success');
            }
        }

        async function loadPrices() {
            const res = await api('/api/prices');
            if (!res) return;
            prices = res;
            const grid = document.getElementById('priceGrid');
            grid.innerHTML = Object.entries(res).map(([id, info]) => `
                <div class="coin-card">
                    <div class="symbol">${info.symbol}</div>
                    <div style="font-size:12px;color:#64748b;margin-top:2px;">${id}</div>
                    <div class="price">$${info.price.toLocaleString(undefined, {minimumFractionDigits:2,maximumFractionDigits:4})}</div>
                </div>
            `).join('');
        }

        async function loadCoins() {
            const coins = await api('/api/coins');
            if (!coins) return;
            const sel = document.getElementById('coinSelect');
            sel.innerHTML = coins.map(c => `<option value="${c.id}">${c.symbol} (${c.id})</option>`).join('');
        }

        async function createAlert() {
            if (!currentUserId) return showToast('请先注册用户', 'error');
            const coinId = document.getElementById('coinSelect').value;
            const alertType = document.getElementById('alertType').value;
            const targetPrice = parseFloat(document.getElementById('targetPrice').value);
            if (!targetPrice || targetPrice <= 0) return showToast('请输入有效价格', 'error');
            
            const res = await api('/api/alerts', {
                method: 'POST',
                body: JSON.stringify({ user_id: currentUserId, coin_id: coinId, alert_type: alertType, target_price: targetPrice })
            });
            if (res) { showToast('告警创建成功'); loadAlerts(); }
        }

        async function loadAlerts() {
            if (!currentUserId) return;
            const alerts = await api('/api/alerts/' + currentUserId);
            if (!alerts) return;
            const tbody = document.getElementById('alertTable');
            if (alerts.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" class="empty">暂无告警，创建一个吧</td></tr>';
                return;
            }
            tbody.innerHTML = alerts.map(a => {
                const currentPrice = prices[a.coin_id] ? `$${prices[a.coin_id].price.toLocaleString()}` : '--';
                return `<tr>
                    <td><strong>${a.coin_symbol}</strong></td>
                    <td><span class="badge badge-${a.alert_type}">${a.alert_type === 'above' ? '↑ 高于' : '↓ 低于'}</span></td>
                    <td>$${a.target_price.toLocaleString()}</td>
                    <td>${currentPrice}</td>
                    <td><span class="badge badge-active">${a.is_active ? '活跃' : '关闭'}</span></td>
                    <td><button class="btn-danger" onclick="deleteAlert(${a.id})">删除</button></td>
                </tr>`;
            }).join('');
        }

        async function deleteAlert(id) {
            await api('/api/alerts/' + id, { method: 'DELETE' });
            showToast('告警已删除');
            loadAlerts();
        }

        let selectedPlan = '';
        async function upgrade(plan) {
            if (!currentUserId) return showToast('请先注册用户', 'error');
            selectedPlan = plan;
            const plans = await api('/api/plans');
            if (!plans) return;
            const info = plans.plans[plan];
            document.getElementById('paymentSection').style.display = 'block';
            document.getElementById('paymentInfo').innerHTML = `
                <h3 style="margin-bottom:12px;">升级到 ${plan.toUpperCase()} - $${info.price_usd}/月</h3>
                <p style="color:#94a3b8;margin:8px 0;">请转账到以下钱包地址：</p>
                <div style="background:#0f172a;padding:16px;border-radius:8px;margin:12px 0;">
                    <p style="font-size:12px;color:#64748b;">网络</p>
                    <p style="font-weight:600;">${plans.chain}</p>
                    <p style="font-size:12px;color:#64748b;margin-top:8px;">钱包地址</p>
                    <code style="font-size:12px;word-break:break-all;">${plans.wallet}</code>
                    <p style="font-size:12px;color:#64748b;margin-top:8px;">金额</p>
                    <p style="font-weight:600;color:#22c55e;">$${info.price_usd}</p>
                </div>
                <div class="form-group">
                    <div style="flex:1;">
                        <label>转账交易哈希 (TxHash)</label>
                        <input type="text" id="txHash" style="width:100%;" placeholder="0x...">
                    </div>
                    <button class="btn-primary" onclick="confirmPayment()">确认支付</button>
                </div>
                <p style="color:#f59e0b;font-size:12px;margin-top:8px;">⏳ 转账后粘贴交易哈希，确认后立即升级</p>
            `;
        }

        async function confirmPayment() {
            const txHash = document.getElementById('txHash').value.trim();
            if (!txHash) return showToast('请输入交易哈希', 'error');
            const res = await api('/api/payment/verify', {
                method: 'POST',
                body: JSON.stringify({ user_id: currentUserId, plan: selectedPlan, tx_hash: txHash })
            });
            if (res) {
                showToast('🎉 升级成功！感谢支持');
                document.getElementById('paymentSection').style.display = 'none';
                loadAlerts();
            }
        }

        // 初始化
        loadCoins();
        loadPrices();
        setInterval(loadPrices, 30000);
        checkAuth();
    </script>
</body>
</html>
"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
