"""
Author: Matthew Akofu
Date Created: Feb 12, 2026
"""

from zoneinfo import ZoneInfo
import os
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
import logging
import sys
import alpaca_trade_api as tradeapi
import trading_view_webhook_helpers 
import strategies
import trade_records
import plot
from alpaca_trade_api.rest import REST, TimeFrame, TimeFrameUnit
from typing import Optional
from pydantic import BaseModel
from datetime import datetime

# All trade, event, and snapshot timestamps are stored in Eastern Time (America/New_York).
# Redis indexes use epoch timestamps derived from those timezone-aware values.

# Configure the root logger.
# logging.INFO Sets the minimum severity level to log. sys.stdout sends logs to stdout. 
logging.basicConfig(
	level=logging.INFO,
	stream=sys.stdout,
	format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("tv-webhook") # Creates a named logger. Akin to a namespace for logs. Helps ID src of logs. 
# So logs will show tv-webhook: message here
logger.setLevel(logging.INFO) # Root logger is already INFO. Ensures this logger also respects INFO.
logger.propagate = True # Logs from tv-webhook -> also sent to parent (root logger) -> stdout. False means logs don't reach root logger.
# Logs appear in docker logs, systemd journal.

# These env vars are injected via the user_data.sh shell script when the EC2 instance is created.
TV_WEBHOOK_SECRET = os.environ["TV_WEBHOOK_SECRET"]  # Required
# Example optional vars:
APCA_API_BASE_URL = os.environ["APCA_API_BASE_URL"]
APCA_API_KEY_ID = os.environ["APCA_API_KEY_ID"]
APCA_API_SECRET_KEY = os.environ["APCA_API_SECRET_KEY"]
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
TV_MAXLEN = int(os.getenv("TV_MAXLEN", "500"))

#PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
#EASTERN_TZ = ZoneInfo("America/New_York")
#MY_TZ = EASTERN_TZ :)

alpaca_api = tradeapi.REST(
    base_url=APCA_API_BASE_URL,
    key_id=APCA_API_KEY_ID,
    secret_key=APCA_API_SECRET_KEY
)
POSITION_SIZE = 200

app = FastAPI(title="TradingView Webhook")

# Instantiate external classes.
tvw_helpers = trading_view_webhook_helpers.TradingViewWebhookHelpers(TV_WEBHOOK_SECRET, REDIS_URL)
trade_recs = trade_records.TradeRecords(tvw_helpers)
stgs = strategies.Strategies(tvw_helpers, trade_recs)
plotter = plot.Plot()

#SECURITIES = ["AAPL", "MSFT", "TSLA", "XOM"]

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
		account = alpaca_api.get_account()
		if account.trading_blocked:
			logger.info("Account is currently restricted from trading")
	except Exception as exc:
		logger.exception("Alpaca get_account failed during startup")
		raise RuntimeError("Alpaca get_account failed during startup") from exc			

@app.get("/health")
def health():
	rr = tvw_helpers.require_redis()
	try:
		rr.ping()
		return {"ok": True, "redis": "up"}
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
	"""
	Handles the TV webhook when it is received by this FastAPI app.
	"""
	rr = tvw_helpers.require_redis()

	if payload.secret != TV_WEBHOOK_SECRET: # Ensure secret embedded in TV payload matches our env secret
		raise HTTPException(status_code=401, detail="Invalid secret")

	tf = tvw_helpers.normalize_tf(payload.timeframe)
	symbol = str(payload.symbol or "").upper().strip()
	#signal = tvw_helpers.normalize_signal(payload.signal)
	signal = payload.signal
	bar_close_time_raw = str(payload.bar_close_time or "").strip()

	if not tf or not symbol or not signal or not bar_close_time_raw:
	#if not tf or not symbol or not signal:
		logger.warning(
			"Invalid webhook payload: timeframe=%r symbol=%r signal=%r bar_close_time=%r",
			payload.timeframe,
			payload.symbol,
			payload.signal,
			payload.bar_close_time,
		)
		raise HTTPException(status_code=400, detail="Missing/invalid timeframe, symbol, or signal or bar close time")		

	acquired, dedupe_key = tvw_helpers.acquire_alert_idempotency(
		symbol=symbol,
		timeframe=tf,
		signal=signal,
		bar_close_time=bar_close_time_raw,
	)

	if not acquired:

		existing = rr.get(dedupe_key)

		# Another worker/process may still be handling it.
		if existing == "processing":
			logger.info(
				"TradingView alert already in progress: symbol=%s tf=%s signal=%s bar_close_time=%s dedupe_key=%s",
				symbol,
				tf,
				signal,
				bar_close_time_raw,
				dedupe_key,
			)
			return {
				"ok": True,
				"duplicate": True,
				"processed": False,
				"in_progress": True,
				"symbol": symbol,
				"timeframe": tf,
				"signal": signal,
				"bar_close_time": bar_close_time_raw,
			}

		# Previously completed successfully.
		if existing and existing.startswith("done:"):
			existing_stream_id = existing.split("done:", 1)[1]
			logger.info(
				"Duplicate TradingView alert ignored: symbol=%s tf=%s signal=%s bar_close_time=%s dedupe_key=%s stream_id=%s",
				symbol,
				tf,
				signal,
				bar_close_time_raw,
				dedupe_key,
				existing_stream_id,
			)
			return {
				"ok": True,
				"duplicate": True,
				"processed": False,
				"in_progress": False,
				"symbol": symbol,
				"timeframe": tf,
				"signal": signal,
				"bar_close_time": bar_close_time_raw,
				"stream_id": existing_stream_id,
			}

		# Fallback for unexpected dedupe value.
		logger.info(
			"Duplicate TradingView alert ignored with unexpected dedupe state: symbol=%s tf=%s signal=%s bar_close_time=%s dedupe_key=%s value=%r",
			symbol,
			tf,
			signal,
			bar_close_time_raw,
			dedupe_key,
			existing,
		)
		return {
			"ok": True,
			"duplicate": True,
			"processed": False,
			"symbol": symbol,
			"timeframe": tf,
			"signal": signal,
			"bar_close_time": bar_close_time_raw,
		}
	
	stream_key = tvw_helpers.stream_key(tf, symbol)
	state_key = tvw_helpers.state_key(tf, symbol)

	received_at = tvw_helpers.utc_now_iso()
	bar_close_time_eastern = tvw_helpers.parse_iso_to_eastern(bar_close_time_raw)

	stream_fields = {
		"symbol": symbol,
		"timeframe": tf,
		"signal": signal,
		"bar_close_time_eastern": tvw_helpers.to_str(bar_close_time_eastern),
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
		"bar_close_time_eastern": tvw_helpers.to_str(bar_close_time_eastern),
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
		pipe.xadd(# Add fields specified by the stream key (e.g. tv:stream:1m:AAPL) to history
			name=stream_key,
			fields=stream_fields,
			maxlen=TV_MAXLEN,
			approximate=True,
		)
		pipe.hset(state_key, mapping=state_fields)
		results = pipe.execute()
		stream_id = results[0]
		rr.set(
			dedupe_key,
			f"done:{stream_id}",
			xx=True,
			ex=tvw_helpers.alert_dedupe_ttl_seconds,
		)
	except Exception:
		try:
			rr.delete(dedupe_key)
		except Exception:
			logger.exception("Failed to clear idempotency key after processing failure")		
		
		logger.exception("Redis write failed")
		raise HTTPException(status_code=500, detail="Redis write failed")			

	logger.info(
		"\n{\n[TV] recv_utc=%s\nsymbol=%s\ntf=%s\nsignal=%s\nbar_close_time_eastern=%s\nprice=%s\nopen=%s\nhigh=%s\nlow=%s\nclose=%s\nvolume=%s\n}\n",
		received_at,
		symbol,
		tf,
		signal.upper(),
		bar_close_time_eastern,
		payload.price,
		payload.open,
		payload.high,
		payload.low,
		payload.close,
		payload.volume,
	)

	#second_last_alert = tvw_helpers.handle_alert(symbol, tf, signal) # Str Str Str
	#logger.info("In app.py: Second last alert = %r", second_last_alert)
	#market_prices = trade_recs.get_market_prices(SECURITIES, alpaca_api)
	#stgs.implement_simple_strategy("simple strategy", True, signal, market_prices.get(symbol, {}).get("market"), symbol, tf)


	now_et = tvw_helpers._now_et()
	if tvw_helpers.is_between_8pm_sun_and_8pm_fri_et(now_et):
		if tvw_helpers.is_symbol_tradable_now(alpaca_api, symbol, now_et):

			prices = trade_recs.get_market_prices([symbol], alpaca_api)
			market_price = prices.get(symbol, {}).get("market")
			if market_price is None or market_price <= 0:
				raise HTTPException(status_code=400, detail="Invalid price")

			NUM_SHARES = POSITION_SIZE/market_price
			
			stgs.entry_strategy1("strategy1", False, now_et, signal, prices, symbol, tf, NUM_SHARES, alpaca_api) 
			stgs.exit_strategy1("strategy1", False, now_et, signal, prices, symbol, tf, alpaca_api)
			stgs.entry_strategy2("strategy2", True, now_et, signal, prices, symbol, tf, NUM_SHARES, alpaca_api)
			stgs.exit_strategy2("strategy2", True, now_et, signal, prices, symbol, tf, alpaca_api)


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
	Retrieves the last nth alert received from TV for the ticker/timeframe pair specified (default, 2nd to the last entry).
	ticker is required. Must be a string of at least 1 char.
	tf is requrired. Must be a non-empty string.
	n is optional because default is 2. Must be >= 1.
	curl "http://localhost:8000/retrieve_nth_last_alert?ticker=AAPL&tf=1m&n=2"
	via Nginx on port 80 get second to the last entry: curl "http://localhost/retrieve_nth_last_alert?ticker=AAPL&tf=1m&n=2"
	Most recent: curl "http://localhost:8000/retrieve_nth_last_alert?ticker=AAPL&tf=1m&n=1"
	Normalization: curl "http://localhost:8000/retrieve_nth_last_alert?ticker=aapl&tf=1&n=2"
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

@app.get("/trades")
def get_trades(
	#start: Optional[datetime] = Query(None),
	#end: Optional[datetime] = Query(None),
	start: Optional[str] = Query(None, description="ISO start datetime"),
	end: Optional[str] = Query(None, description="ISO end datetime"),	
	tickers: Optional[list[str]] = Query(None, description="Ticker filter"),
):
	"""
	Returns trade records between dates and tickers specified.
		start (str iso format!): Optional. Start date.
		end (str iso format!): Optional. End date.
		tickers (list): List of ticker symbols.
	Example call: 
		curl "http://localhost:8000/trades?start=2026-03-10T00:00:00Z&end=2026-03-11T00:00:00Z&tickers=AAPL&tickers=MSFT"
	"""
	try:
		# If start missing -> get earliest trade
		if start is None:
			start = trade_recs.get_first_trade_time()

		# If end missing -> get latest trade
		if end is None:
			end = trade_recs.get_last_trade_time()

		if not start or not end:
			return {
				"start": start,
				"end": end,
				"count": 0,
				"records": []
			}			

		records = trade_recs.get_trade_records_between(start, end, tickers=tickers)

	except ValueError:
		raise HTTPException(status_code=400, detail="Invalid ISO date range")

	return {
		"start": start,
		"end": end,
		"count": len(records),
		"records": records,
	}	

@app.get("/debug/state/{timeframe}/{symbol}")
async def debug_state_symbol(
	timeframe: str,
	symbol: str,
	fields: str = Query(
		default="symbol,timeframe,signal,bar_close_time_eastern,received_at,price,open,high,low,close,volume,stream_key"
	),
):
	"""
	Debug endpoint that lets us query the current Redis “state” for a symbol/timeframe, and optionally choose which fields to return.
	Good for debugging webhook ingestion, verifying Redis writes, checking latest signal, inspecting OHLC data.
	Example calls:
		From EC2:
			curl "http://localhost:8000/debug/state/15m/AAPL" uses default fiels: symbol,timeframe,signal,bar_close_time,...
			curl "http://localhost:8000/debug/state/15m/AAPL?fields=price,signal"  specifies custom fields
		From laptop:
			curl "http://<EC2_PUBLIC_IP>/debug/state/15m/AAPL?fields=price,signal"
	"""
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
	"""
	Returns the most recent events (history) -last N- from a Redis stream for a given timeframe/ticker pair.
	Good for debuging webhook ingestion, inspecting signal history, verifying ordering of events, 
	replaying recent trades mentally, validating Redis stream integrity.
	Example calls:
		From EC2:
			curl "http://localhost/debug/stream/15m/AAPL"
		From laptop:
			curl "http://<EC2_PUBLIC_IP>/debug/stream/15m/AAPL"
			curl "http://<EC2_PUBLIC_IP>/debug/stream/15m/AAPL?count=5"
		From Browser:
			http://<EC2_PUBLIC_IP>/debug/stream/15m/AAPL?count=10
	"""
	rr = tvw_helpers.require_redis()

	tf = tvw_helpers.normalize_tf(timeframe)
	sym = str(symbol or "").upper().strip()

	if not tf or not sym:
		raise HTTPException(status_code=400, detail="Missing/invalid timeframe or symbol")		

	key = tvw_helpers.stream_key(tf, sym)

	if not rr.exists(key):
		raise HTTPException(404, "Stream not found")	

	try:
		entries = rr.xrevrange(key, max="+", min="-", count=count)
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
	"""
	Returns historical events from the Redis stream (oldest -> newest) for a given timeframe/ticker pair.
	Useful instead of /debug/stream when we want earliest events, chronological order, to replay from the beginning
	to inspect initial signals.
	Example calls:
		From EC2:
			curl "http://localhost/debug/stream-range/15m/AAPL"
		From laptop:
			curl "http://<EC2_PUBLIC_IP>/debug/stream-range/15m/AAPL"
			curl "http://<EC2_PUBLIC_IP>/debug/stream-range/15m/AAPL?count=10"
		From browser:
			http://<EC2_PUBLIC_IP>/debug/stream-range/15m/AAPL?count=50
	"""
	rr = tvw_helpers.require_redis()

	tf = tvw_helpers.normalize_tf(timeframe)
	sym = str(symbol or "").upper().strip()

	if not tf or not sym:
		raise HTTPException(status_code=400, detail="Missing/invalid timeframe or symbol")

	key = tvw_helpers.stream_key(tf, sym)

	if not rr.exists(key):
		raise HTTPException(404, "Stream not found")	

	try:
		entries = rr.xrange(key, min="-", max="+", count=count)
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



@app.post("/pnl/snapshot/run")
def run_pnl_snapshot(
	strategy_name: str = Query(..., min_length=1),
):
	"""
	Triggers a PnL snapshot calculation for a given strategy. Used to manually trigger PnL calculation,
	test our strategy performance, debug pricing + trade records, verify our cron job behavior.
	Example calls:
		From EC2:
			curl -X POST "http://localhost/pnl/snapshot/run?strategy_name=simple%20strategy"
		From laptop:
			curl -X POST "http://<EC2_PUBLIC_IP>/pnl/snapshot/run?strategy_name=simple%20strategy"
	"""
	try:
		result = trade_recs.snapshot_pnl(strategy_name, alpaca_api)
	except Exception:
		logger.exception("PnL snapshot failed")
		raise HTTPException(status_code=500, detail="PnL snapshot failed")

	return result


@app.get("/pnl/history")
def get_pnl_history(
	strategy_name: str = Query(..., min_length=1),
	start: Optional[str] = Query(default=None),
	end: Optional[str] = Query(default=None),
	ticker: Optional[str] = Query(default=None),
):
	"""
	Retrieves historical PnL snapshots for a given strategy, with optional filters.
	Example calls:
		From laptop:
			Basic call (all history):
				curl "http://<EC2_PUBLIC_IP>/pnl/history?strategy_name=simple%20strategy"   
			With time range:
				curl "http://<EC2_PUBLIC_IP>/pnl/history?strategy_name=simple%20strategy&start=2026-03-10T00:00:00Z&end=2026-03-11T00:00:00Z"
			Filter by ticker:
				curl "http://<EC2_PUBLIC_IP>/pnl/history?strategy_name=simple%20strategy&ticker=AAPL"
			Combine tickers:
				curl "http://<EC2_PUBLIC_IP>/pnl/history?strategy_name=simple%20strategy&start=2026-03-10T00:00:00Z&end=2026-03-11T00:00:00Z&ticker=AAPL"
		From EC2:
			basic call:
				curl "http://localhost/pnl/history?strategy_name=simple%20strategy"
			Agg history for a date range
				curl "http://localhost:8000/pnl/history?strategy_name=simple%20strategy&start=2026-03-01T00:00:00Z&end=2026-03-13T23:59:59Z"  
	"""
	try:
		history = trade_recs.get_pnl_history(
			strategy_name=strategy_name,
			start=start,
			end=end,
			ticker=ticker,
		)
	except ValueError:
		raise HTTPException(status_code=400, detail="Invalid ISO date range")
	except Exception:
		logger.exception("PnL history retrieval failed")
		raise HTTPException(status_code=500, detail="PnL history retrieval failed")

	return {
		"strategy_name": strategy_name,
		"start": start,
		"end": end,
		"ticker": ticker,
		"count": len(history),
		"history": history,
	}


@app.get("/pnl/plot")
def get_pnl_plot(
	strategy_name: str = Query(..., min_length=1),
	start: Optional[str] = Query(default=None),
	end: Optional[str] = Query(default=None),
	ticker: Optional[str] = Query(default=None),
):
	"""
	Plot aggregate PNL
	curl "http://localhost:8000/pnl/plot?strategy_name=simple%20strategy" --output pnl.png
	"""
	try:
		history = trade_recs.get_pnl_history(
			strategy_name=strategy_name,
			start=start,
			end=end,
			ticker=ticker,
		)
	except ValueError:
		raise HTTPException(status_code=400, detail="Invalid ISO date range")
	except Exception:
		logger.exception("PnL plot history retrieval failed")
		raise HTTPException(status_code=500, detail="PnL plot history retrieval failed")

	if not history:
		raise HTTPException(status_code=404, detail="No PnL history found for requested filters")

	title = f"PnL History - {strategy_name}"
	if ticker:
		title = f"PnL History - {strategy_name} - {ticker.upper().strip()}"
	else:
		title = f"PnL History - {strategy_name} - Aggregate"

	try:
		image_buffer = plotter.plot_pnl_history(history, title=title)
	except Exception:
		logger.exception("PnL plot generation failed")
		raise HTTPException(status_code=500, detail="PnL plot generation failed")

	return StreamingResponse(image_buffer, media_type="image/png")


@app.get("/trade-events")
def get_trade_events(
	strategy_name: Optional[str] = Query(default=None),
	ticker: Optional[str] = Query(default=None),
	start: Optional[str] = Query(default=None),
	end: Optional[str] = Query(default=None),
):	
	"""
	Get individual trade events
	curl "http://localhost:8000/trade-events?strategy_name=simple%20strategy&ticker=AAPL"
	"""
	try:
		events = trade_recs.get_trade_events(
			strategy_name=strategy_name,
			ticker=ticker,
			start=start,
			end=end,
		)
	except ValueError:
		raise HTTPException(status_code=400, detail="Invalid ISO date range")
	except Exception:
		logger.exception("Trade event retrieval failed")
		raise HTTPException(status_code=500, detail="Trade event retrieval failed")

	return {
		"strategy_name": strategy_name,
		"ticker": ticker,
		"start": start,
		"end": end,
		"count": len(events),
		"events": events,
	}


@app.post("/debug/reset-redis")
def reset_redis():
	"""
	Reset all app redis data
	curl -X POST "http://localhost:8000/debug/reset-redis"
	"""
	try:
		result = trade_recs.reset_tv_data()
	except Exception:
		logger.exception("Redis reset failed")
		raise HTTPException(status_code=500, detail="Redis reset failed")

	return {
		"ok": True,
		**result,
	}

@app.post("/pnl/snapshot/run-all")
def run_all_pnl_snapshots():
	"""
	ssh into EC2
	crontab -e
	0,15,30,45 * * * * curl -fsS -X POST "http://localhost/pnl/snapshot/run-all" >/tmp/trading-mage-pnl-cron.log 2>&1
	0,15,30,45 * * * * curl -fsS -X POST "http://localhost/pnl/snapshot/run-all" >>/var/log/trading-mage-pnl-cron.log 2>&1  
	Second version is slightly better as it appends instead of overwrites. But must first create the file:
	touch /var/log/trading-mage-pnl-cron.log
	
	Or better yet, use this simple version to avoid permission issues:
	crontab -e
	0,15,30,45 * * * * curl -fsS -X POST "http://localhost/pnl/snapshot/run-all" >>/home/ubuntu/trading-mage-pnl-cron.log 2>&1
	Then verity cron loaded
	crontab -l
	Check cron service: sudo systemctl status cron  Or Ubuntu if needed: sudo systemctl enable --now cron
	Test manually b4 waiting 15 minutes:
	curl -X POST "http://localhost/pnl/snapshot/run-all"
	Then inspect Redis back history through API
	curl "http://localhost/pnl/history?strategy_name=simple%20strategy"
	And trade events
	curl "http://localhost/trade-events?strategy_name=simple%20strategy"
	"""
	try:
		result = trade_recs.snapshot_all_pnl(alpaca_api)
	except Exception:
		logger.exception("All-strategy PnL snapshot failed")
		raise HTTPException(status_code=500, detail="All-strategy PnL snapshot failed")

	return result	

