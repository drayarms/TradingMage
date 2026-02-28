"""
Author: Matthew Akofu
Date Created: Feb 12, 2026
"""


#import numpy #as np
#import pandas #as pd
#from pytz import timezone
from zoneinfo import ZoneInfo
#import time
import config 
import dataframes

import json
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException
import logging


#import os
#import os.path

SECURITIES = []

#import math


# Get our account information.
account = config.api.get_account()

#PACIFIC_TZ = timezone('US/Pacific')
#EASTERN_TZ = timezone('US/Eastern')
PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
EASTERN_TZ = ZoneInfo("America/New_York")
MY_TZ = EASTERN_TZ


# Check if our account is restricted from trading.
if account.trading_blocked:
    #print('Account is currently restricted from trading.')
    logger.info("Account is currently restricted from trading")


logger = logging.getLogger("tv-webhook")
logger.setLevel(logging.INFO)




if not config.TV_WEBHOOK_SECRET:
	raise RuntimeError("Missing TV_WEBHOOK_SECRET environment variable")

app = FastAPI(title="TradingView Webhook - Print Signals Only")


def _utc_now_iso() -> str:
	return datetime.now(timezone.utc).isoformat()


def _normalize_tf(tf: str) -> str:
	"""
	Normalizes TradingView timeframe strings to a consistent label.
	Examples:
	- "15" -> "15m"
	- "60" -> "1h"
	- "240" -> "4h"
	- "15m" stays "15m"
	- "4h" stays "4h"
	"""
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


def _utc_iso_to_pacific(iso_str: str) -> str:
	if not iso_str:
		return ""

	# Handle "Z" suffix from TradingView
	if iso_str.endswith("Z"):
		iso_str = iso_str.replace("Z", "+00:00")

	try:
		dt_utc = datetime.fromisoformat(iso_str)
		dt_pacific = dt_utc.astimezone(PACIFIC_TZ)
		return dt_pacific.isoformat()
	except Exception:
		return iso_str  # fallback, don't break webhook	


@app.get("/health")
def health():
	return {"ok": True}


@app.post("/webhook/tradingview")
async def tradingview_webhook(request: Request):
	payload = await request.json()

	# 1) Auth
	if payload.get("secret") != config.TV_WEBHOOK_SECRET: #Ensure secret obtained from payload matches the configured secret to preven some random payload from accessing app
		logger.warning("Invalid webhook secret")
		raise HTTPException(status_code=401, detail="Invalid secret")


	# 2) Parse
	symbol = str(payload.get("symbol", "")).upper()
	timeframe = _normalize_tf(str(payload.get("timeframe", "")))
	signal = str(payload.get("signal", "")).lower()
	bar_close_time = str(payload.get("bar_close_time", ""))

	price = payload.get("price", None)
	if price is not None:
		try:
			price = float(price)
		except Exception:
			pass

	# 3) Validate minimal fields
	if not symbol or not timeframe or signal not in ("buy", "sell") or not bar_close_time:
		raise HTTPException(status_code=400, detail="Missing/invalid fields")

	# 4) Print nicely
	bar_close_time_pacific = _utc_iso_to_pacific(bar_close_time)	
	#print(
		#f"[TV] recv_utc={_utc_now_iso()} | "
		#f"symbol={symbol} | tf={timeframe} | signal={signal.upper()} | "
		#f"bar_close_time={bar_close_time_pacific} | price={price}"
	#)
	#logger.info(
		#"TradingView signal received",
		#extra={
			#"recv_utc": _utc_now_iso(),
			#"symbol": symbol,
			#"timeframe": timeframe,
			#"signal": signal.upper(),
			#"bar_close_time": bar_close_time_pacific,
			#"price": price,
		#},
	#)	

	logger.info(
		"[TV] recv_utc=%s | symbol=%s | tf=%s | signal=%s | bar_close_time=%s | price=%s",
		_utc_now_iso(),
		symbol,
		timeframe,
		signal.upper(),
		bar_close_time_pacific,
		price,
	)	

	# Optional: print raw payload for debugging (comment out once stable)
	# print("[TV] raw:", json.dumps(payload, indent=2, sort_keys=True))

	return {"ok": True, "printed": True}




#if __name__ == "__main__":

	#dataframes_instance = dataframes.Dataframes(MY_TZ)
    #_4hr_df = dataframes_instance.get_df(ticker_symbols, dataframes_instance._4hr_time_frame, start_date, end_date)
