"""
Author: Matthew Akofu
Date Created: Feb 12, 2026
"""

#import numpy #as np 
#import pandas #as pd
#from pytz import timezone
from zoneinfo import ZoneInfo
#import time
#import config 
##REMOVE CONFIG FIRST !!!!!! import dataframes
import os
#import redis

#import json
#from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException, Query
#from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import logging
import sys

import alpaca_trade_api as tradeapi

import trading_view_webhook_helpers 

#import hashlib

from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit

from typing import Optional

from pydantic import BaseModel

logging.basicConfig(
	level=logging.INFO,
	stream=sys.stdout,
	format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("tv-webhook")
logger.setLevel(logging.INFO)
logger.propagate = True

TV_WEBHOOK_SECRET = os.environ["TV_WEBHOOK_SECRET"]  # required
# Example optional vars:
APCA_API_BASE_URL = os.environ["APCA_API_BASE_URL"]
APCA_API_KEY_ID = os.environ["APCA_API_KEY_ID"]
APCA_API_SECRET_KEY = os.environ["APCA_API_SECRET_KEY"]
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
TV_MAXLEN = int(os.getenv("TV_MAXLEN", "500"))


#PACIFIC_TZ = timezone('US/Pacific')
#EASTERN_TZ = timezone('US/Eastern')
PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
EASTERN_TZ = ZoneInfo("America/New_York")
MY_TZ = EASTERN_TZ

api = tradeapi.REST(
    base_url=APCA_API_BASE_URL,
    key_id=APCA_API_KEY_ID,
    secret_key=APCA_API_SECRET_KEY
)

#import os
#import os.path

SECURITIES = []

#import math


# Get our account information.
#account = api.get_account()

# Check if our account is restricted from trading.
#if account.trading_blocked:
    #print('Account is currently restricted from trading.')
    #logger.info("Account is currently restricted from trading")


app = FastAPI(title="TradingView Webhook - Print Signals Only")

tvw_helpers = trading_view_webhook_helpers.TradingViewWebhookHelpers(TV_WEBHOOK_SECRET, REDIS_URL)

class TradingViewWebhook(BaseModel):
	secret: str
	symbol: str
	timeframe: str
	signal: str
	bar_close_time: str
	price: Optional[float] = None
	open: Optional[float] = None
	high: Optional[float] = None
	low: Optional[float] = None
	close: Optional[float] = None
	volume: Optional[float] = None


# When app starts, this function runs once
#systemd -> docker run -> uvicorn app:app -> FastAPI app object loads -> FastAPI startup event fires -> _startup() runs
@app.on_event("startup")
def _startup():
	try:
		account = api.get_account()
		if account.trading_blocked:
			logger.info("Account is currently restricted from trading")
	#except Exception:
		#logger.exception("Alpaca get_account failed during startup")
	except Exception as exc:
		logger.exception("Alpaca get_account failed during startup")
		raise RuntimeError("Alpaca get_account failed during startup") from exc			

@app.get("/health")
def health():
	rr = tvw_helpers.require_redis()

	try:
		rr.ping()
		return {"ok": True, "redis": "up"}
	#except Exception as exc:
		#raise HTTPException(status_code=503, detail=f"Redis ping failed: {exc}")
	except Exception:
		logger.exception("Redis ping failed")
		raise HTTPException(status_code=503, detail="Redis ping failed")		

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
	logger.exception("Unhandled exception")
	return JSONResponse(
		status_code=500,
		content={"detail": "Internal server error"},
	)

@app.post("/webhook/tradingview")
async def webhook_tradingview(payload: TradingViewWebhook):
	rr = tvw_helpers.require_redis()

	if payload.secret != TV_WEBHOOK_SECRET:
		raise HTTPException(status_code=401, detail="Invalid secret")

	tf = tvw_helpers.normalize_tf(payload.timeframe)
	symbol = str(payload.symbol or "").upper().strip()
	signal = tvw_helpers.normalize_signal(payload.signal)

	if not tf or not symbol or not signal:
		logger.warning(
			"Invalid webhook payload: timeframe=%r symbol=%r signal=%r",
			payload.timeframe,
			payload.symbol,
			payload.signal,
		)
		raise HTTPException(status_code=400, detail="Missing/invalid timeframe, symbol, or signal")		

	stream_key = tvw_helpers.stream_key(tf, symbol)
	state_key = tvw_helpers.state_key(tf, symbol)

	received_at = tvw_helpers.utc_now_iso()
	bar_close_time_pacific = tvw_helpers.parse_iso_to_pacific(payload.bar_close_time)

	stream_fields = {
		"symbol": symbol,
		"timeframe": tf,
		"signal": signal,
		"bar_close_time": tvw_helpers.to_str(payload.bar_close_time),
		"bar_close_time_pacific": tvw_helpers.to_str(bar_close_time_pacific),
		"received_at": received_at,
		"price": tvw_helpers.to_str(payload.price),
		"open": tvw_helpers.to_str(payload.open),
		"high": tvw_helpers.to_str(payload.high),
		"low": tvw_helpers.to_str(payload.low),
		"close": tvw_helpers.to_str(payload.close),
		"volume": tvw_helpers.to_str(payload.volume),
	}

	state_fields = {
		"symbol": symbol,
		"timeframe": tf,
		"signal": signal,
		"bar_close_time": tvw_helpers.to_str(payload.bar_close_time),
		"bar_close_time_pacific": tvw_helpers.to_str(bar_close_time_pacific),
		"received_at": received_at,
		"price": tvw_helpers.to_str(payload.price),
		"open": tvw_helpers.to_str(payload.open),
		"high": tvw_helpers.to_str(payload.high),
		"low": tvw_helpers.to_str(payload.low),
		"close": tvw_helpers.to_str(payload.close),
		"volume": tvw_helpers.to_str(payload.volume),
		"stream_key": stream_key,
	}

	try:
		pipe = rr.pipeline()
		pipe.xadd(
			name=stream_key,
			fields=stream_fields,
			maxlen=TV_MAXLEN,
			approximate=True,
		)
		pipe.hset(state_key, mapping=state_fields)
		results = pipe.execute()
		stream_id = results[0]
	#except Exception as exc:
		#raise HTTPException(status_code=500, detail=f"Redis write failed: {exc}")
	except Exception:
		logger.exception("Redis write failed")
		raise HTTPException(status_code=500, detail="Redis write failed")			

	logger.info(
		"\n{\n[TV] recv_utc=%s\nsymbol=%s\ntf=%s\nsignal=%s\nbar_close_time_utc=%s\nbar_close_time_pacific=%s\nprice=%s\nopen=%s\nhigh=%s\nlow=%s\nclose=%s\nvolume=%s\n}\n",
		received_at,
		symbol,
		tf,
		signal.upper(),
		payload.bar_close_time,
		bar_close_time_pacific,
		payload.price,
		payload.open,
		payload.high,
		payload.low,
		payload.close,
		payload.volume,
	)

	second_last_alert = tvw_helpers.handle_alert(symbol, tf, signal) # Str Str Str
	logger.info("In app.py: Second last alert = %r", second_last_alert)

	return {
		"ok": True,
		"printed": True,
		"symbol": symbol,
		"timeframe": tf,
		"signal": signal,
		"stream": stream_key,
		"state": state_key,
		"stream_id": stream_id,
		"maxlen": TV_MAXLEN,
	}


#@app.get("/retrieve_nth_last_alert")
#def retrieve_nth_last_alert():
	#ticker = request.args.get('ticker')
	#tf = request.args.get('tf')
	#tvw_helpers.log_nth_last_alert(ticker, tf, 1)

@app.get("/retrieve_nth_last_alert")
def retrieve_nth_last_alert(
	ticker: str = Query(..., min_length=1),
	tf: str = Query(..., min_length=1),
	n: int = Query(2, ge=1),
):
	"""
	Retrieves the last nth entries for the ticker/timeframe pair specified.
	# curl "http://localhost:8000/retrieve_nth_last_alert?ticker=AAPL&tf=1m&n=2"
	# via Nginx on port 80: curl "http://localhost/retrieve_nth_last_alert?ticker=AAPL&tf=1m&n=2"
	# Most recent: curl "http://localhost:8000/retrieve_nth_last_alert?ticker=AAPL&tf=1m&n=1"
	# Normalization: curl "http://localhost:8000/retrieve_nth_last_alert?ticker=aapl&tf=1&n=2"
	"""
	entry = tvw_helpers.get_nth_last_alert(ticker, tf, n)

	if entry is None:
		raise HTTPException(status_code=404, detail="Not enough alerts found")

	entry_id, fields = entry

	return {
		"ticker": ticker.upper().strip(),
		"timeframe": tvw_helpers.normalize_tf(tf),
		"n": n,
		"entry": {
			"id": entry_id,
			"fields": {
				**fields,
				"price": tvw_helpers.safe_float(fields.get("price")),
				"open": tvw_helpers.safe_float(fields.get("open")),
				"high": tvw_helpers.safe_float(fields.get("high")),
				"low": tvw_helpers.safe_float(fields.get("low")),
				"close": tvw_helpers.safe_float(fields.get("close")),
				"volume": tvw_helpers.safe_float(fields.get("volume")),
			},
		},
	}


@app.get("/debug/state/{timeframe}/{symbol}")
async def debug_state_symbol(
	timeframe: str,
	symbol: str,
	fields: str = Query(
		default="symbol,timeframe,signal,bar_close_time,bar_close_time_pacific,received_at,price,open,high,low,close,volume,stream_key"
	),
):
	rr = tvw_helpers.require_redis()

	tf = tvw_helpers.normalize_tf(timeframe)
	sym = str(symbol or "").upper().strip()

	if not tf or not sym:
		raise HTTPException(status_code=400, detail="Missing/invalid timeframe or symbol")

	field_list = [f.strip() for f in fields.split(",") if f.strip()]
	if not field_list:
		raise HTTPException(status_code=400, detail="No fields requested")

	key = tvw_helpers.state_key(tf, sym)

	if not rr.exists(key):
		raise HTTPException(status_code=404, detail="No state found for symbol/timeframe")

	try:
		values = rr.hmget(key, field_list)
	#except Exception as exc:
		#raise HTTPException(status_code=500, detail=f"Redis read failed: {exc}")
	except Exception:
		logger.exception("Redis read failed")
		raise HTTPException(status_code=500, detail="Redis read failed")			

	data = {field: value for field, value in zip(field_list, values)}

	for numeric_field in ("price", "open", "high", "low", "close", "volume"):
		if numeric_field in data:
			data[numeric_field] = tvw_helpers.safe_float(data[numeric_field])

	return {
		"key": key,
		"data": data,
	}


@app.get("/debug/stream/{timeframe}/{symbol}")
async def debug_stream_symbol(
	timeframe: str,
	symbol: str,
	count: int = Query(default=20, ge=1, le=500),
):
	rr = tvw_helpers.require_redis()

	tf = tvw_helpers.normalize_tf(timeframe)
	sym = str(symbol or "").upper().strip()

	if not tf or not sym:
		raise HTTPException(status_code=400, detail="Missing/invalid timeframe or symbol")

	key = tvw_helpers.stream_key(tf, sym)

	try:
		entries = rr.xrevrange(key, max="+", min="-", count=count)
	#except Exception as exc:
		#raise HTTPException(status_code=500, detail=f"Redis stream read failed: {exc}")
	except Exception:
		logger.exception("Redis stream read failed")
		raise HTTPException(status_code=500, detail="Redis stream read failed")			

	return {
		"stream": key,
		"count": len(entries),
		"entries": [
			{
				"id": entry_id,
				"fields": {
					**fields,
					"price": tvw_helpers.safe_float(fields.get("price")),
					"open": tvw_helpers.safe_float(fields.get("open")),
					"high": tvw_helpers.safe_float(fields.get("high")),
					"low": tvw_helpers.safe_float(fields.get("low")),
					"close": tvw_helpers.safe_float(fields.get("close")),
					"volume": tvw_helpers.safe_float(fields.get("volume")),
				},
			}
			for entry_id, fields in entries
		],
	}


@app.get("/debug/stream-range/{timeframe}/{symbol}")
async def debug_stream_range_symbol(
	timeframe: str,
	symbol: str,
	count: int = Query(default=100, ge=1, le=1000),
):
	rr = tvw_helpers.require_redis()

	tf = tvw_helpers.normalize_tf(timeframe)
	sym = str(symbol or "").upper().strip()

	if not tf or not sym:
		raise HTTPException(status_code=400, detail="Missing/invalid timeframe or symbol")

	key = tvw_helpers.stream_key(tf, sym)

	try:
		entries = rr.xrange(key, min="-", max="+", count=count)
	#except Exception as exc:
		#raise HTTPException(status_code=500, detail=f"Redis stream read failed: {exc}")
	except Exception:
		logger.exception("Redis stream read failed")
		raise HTTPException(status_code=500, detail="Redis stream read failed")			

	return {
		"stream": key,
		"count": len(entries),
		"entries": [
			{
				"id": entry_id,
				"fields": {
					**fields,
					"price": tvw_helpers.safe_float(fields.get("price")),
					"open": tvw_helpers.safe_float(fields.get("open")),
					"high": tvw_helpers.safe_float(fields.get("high")),
					"low": tvw_helpers.safe_float(fields.get("low")),
					"close": tvw_helpers.safe_float(fields.get("close")),
					"volume": tvw_helpers.safe_float(fields.get("volume")),
				},
			}
			for entry_id, fields in entries
		],
	}

