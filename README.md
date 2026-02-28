TradingMage – TradingView LuxAlgo Webhook Integration
Overview

This project enables TradingView LuxAlgo alerts to send webhook payloads to a publicly reachable FastAPI service, which receives, validates, and processes trading signals for automated or semi-automated trading workflows.

At minimum, this setup:

Receives BUY/SELL signals from TradingView

Authenticates requests using a shared secret

Logs signals in a structured, readable format

Supports both local testing via ngrok and production deployment on AWS EC2

Requirements
Accounts & Subscriptions

TradingView account with LuxAlgo subscription

Publicly reachable URL (ngrok or EC2)

AWS account (for production deployment)

Software

Python 3.9+

FastAPI + Uvicorn

ngrok (for local testing)

AWS CLI (for EC2 / Terraform workflows)

Goals

Install TradingView LuxAlgo alerts that POST JSON payloads to a webhook

FastAPI application listens for alerts

TradingMage processes signals and makes trading decisions (future extension)

Architecture (High Level)
Local Testing
TradingView → ngrok HTTPS URL → FastAPI (localhost:8000)
Production
TradingView → EC2 Public IP / ALB → Nginx → FastAPI (Docker)
Minimal Local Setup
1. Create a Virtual Environment
python3 -m venv .venv

Directory structure:

Project_Dir/
├── .venv/
│   ├── bin/
│   ├── lib/
│   └── ...
└── TradingMage/

Add to .gitignore:

.venv/
2. Activate the Virtual Environment
source .venv/bin/activate

Prompt should look like:

(.venv) user@host ProjectDir %
3. Install Dependencies
pip install fastapi uvicorn alpaca-py alpaca-trade-api werkzeug

Verify:

pip list

Deactivate when finished:

deactivate
Webhook Secret
Generate a Secret
python3 -c "import secrets; print(secrets.token_urlsafe(48))"
Export as Environment Variable
export TV_WEBHOOK_SECRET="PASTE_THE_GENERATED_STRING_HERE"

This secret must match the value sent by TradingView alerts.

FastAPI Webhook Application

Path: TradingMage/app/app.py

from zoneinfo import ZoneInfo
import config
import json
from datetime import datetime, timezone
from fastapi import FastAPI, Request, HTTPException

account = config.api.get_account()

PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
EASTERN_TZ = ZoneInfo("America/New_York")
MY_TZ = EASTERN_TZ

if account.trading_blocked:
    print("Account is currently restricted from trading.")

if not config.TV_WEBHOOK_SECRET:
    raise RuntimeError("Missing TV_WEBHOOK_SECRET environment variable")

app = FastAPI(title="TradingView Webhook - Print Signals Only")

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _normalize_tf(tf: str) -> str:
    t = (tf or "").strip().lower()
    if not t:
        return ""
    if t.endswith(("m", "h", "d")):
        return t
    if t.isdigit():
        mins = int(t)
        if mins == 60:
            return "1h"
        if mins == 240:
            return "4h"
        return f"{mins}m"
    return t

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/webhook/tradingview")
async def tradingview_webhook(request: Request):
    payload = await request.json()

    if payload.get("secret") != config.TV_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid secret")

    symbol = str(payload.get("symbol", "")).upper()
    timeframe = _normalize_tf(str(payload.get("timeframe", "")))
    signal = str(payload.get("signal", "")).lower()
    bar_close_time = str(payload.get("bar_close_time", ""))

    price = payload.get("price")
    if price is not None:
        try:
            price = float(price)
        except Exception:
            pass

    if not symbol or not timeframe or signal not in ("buy", "sell") or not bar_close_time:
        raise HTTPException(status_code=400, detail="Missing/invalid fields")

    print(
        f"[TV] recv_utc={_utc_now_iso()} | "
        f"symbol={symbol} | tf={timeframe} | signal={signal.upper()} | "
        f"bar_close_time={bar_close_time} | price={price}"
    )

    return {"ok": True}
Local Testing with ngrok
Install ngrok

macOS (Homebrew)

brew install ngrok/ngrok/ngrok

Linux

curl -s https://ngrok-agent.s3.amazonaws.com/ngrok.asc \
| sudo tee /etc/apt/trusted.gpg.d/ngrok.asc >/dev/null

echo "deb https://ngrok-agent.s3.amazonaws.com buster main" \
| sudo tee /etc/apt/sources.list.d/ngrok.list

sudo apt update && sudo apt install ngrok
Authenticate ngrok (One-Time)
ngrok config add-authtoken YOUR_TOKEN

Token is stored globally:

~/Library/Application Support/ngrok/ngrok.yml
Run FastAPI (Inside venv)
uvicorn app:app --host 0.0.0.0 --port 8000

Leave this running.

Start ngrok (Outside venv)
ngrok http 8000

Example output:

Forwarding https://abcd-1234.ngrok.io -> http://localhost:8000
Test Locally
curl -X POST http://localhost:8000/webhook/tradingview \
  -H "Content-Type: application/json" \
  -d '{
    "secret": "YOUR_TV_WEBHOOK_SECRET",
    "symbol": "AAPL",
    "timeframe": "1m",
    "signal": "buy",
    "bar_close_time": "2026-02-15T23:59:00Z",
    "price": 187.12
  }'

Open ngrok inspector:

http://127.0.0.1:4040
Production: AWS EC2 Deployment (Summary)

Ubuntu 22.04 EC2 (t3.micro)

Security Group:

SSH (22) from your IP

TCP 8000 from 0.0.0.0/0 (temporary)

Python + venv + FastAPI

uvicorn --host 0.0.0.0 --port 8000

Health check:

curl http://EC2_PUBLIC_IP:8000/health

Webhook URL:

http://EC2_PUBLIC_IP:8000/webhook/tradingview
Logging & Observability

FastAPI uses Python logging

Uvicorn captures stdout/stderr

In production:

Docker logs

systemd journal

View logs:

sudo journalctl -u tv-webhook.service -f
sudo docker logs -f tv-webhook
TradingView Alert Configuration
Create Alert

Open chart

Add LuxAlgo indicator

Open Alerts → Create Alert

Condition:

LuxAlgo → BUY or SELL signal

Trigger:

Once per bar close

Webhook Payload (BUY Example)
{
  "secret": "YOUR_SECRET",
  "symbol": "{{ticker}}",
  "timeframe": "{{interval}}",
  "signal": "buy",
  "bar_close_time": "{{time}}",
  "price": {{close}}
}

For SELL alerts:

"signal": "sell"
Notes & Warnings

⚠️ ngrok URLs change on restart (testing only)

Never expose secrets in URLs

TradingView alerts run server-side once created

This setup prints signals only — execution logic is intentionally decoupled

Next Steps (Optional)

Persist signals to a database

Add strategy engine

Enforce symbol/timeframe allowlists

Add rate-limiting & IP filtering

Move webhook behind ALB + HTTPS