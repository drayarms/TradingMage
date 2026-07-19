"""
Author: Matthew Akofu
Date Created: Feb 12, 2026
"""

import os
import logging
import sys
from typing import Optional
from pydantic import BaseModel

from fastapi import FastAPI, Request, HTTPException, Query, BackgroundTasks
#from fastapi.responses import JSONResponse, StreamingResponse, PlainTextResponse
from fastapi.responses import (
	JSONResponse,
	StreamingResponse,
	PlainTextResponse,
)

import alpaca_trade_api as tradeapi

import trading_view_webhook_helpers
import strategies
import trade_records
import backtester
import plot


# All trade, event, and snapshot timestamps are stored in Eastern Time (America/New_York).
# Redis indexes use epoch timestamps derived from those timezone-aware values. Git change.

logging.basicConfig(
	level=logging.INFO,
	stream=sys.stdout,
	format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("tv-webhook")
logger.setLevel(logging.INFO)
logger.propagate = True

TV_WEBHOOK_SECRET = os.environ["TV_WEBHOOK_SECRET"]
#APCA_API_BASE_URL = os.environ["APCA_API_BASE_URL"]
#APCA_API_KEY_ID = os.environ["APCA_API_KEY_ID"]
#APCA_API_SECRET_KEY = os.environ["APCA_API_SECRET_KEY"]
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
TV_MAXLEN = int(os.getenv("TV_MAXLEN", "500"))

APCA_API_BASE_URL_STG1_15M = os.environ["APCA_API_BASE_URL_STG1_15M"]
APCA_API_KEY_ID_STG1_15M = os.environ["APCA_API_KEY_ID_STG1_15M"]
APCA_API_SECRET_KEY_STG1_15M = os.environ["APCA_API_SECRET_KEY_STG1_15M"]

APCA_API_BASE_URL_STG1_1H = os.environ["APCA_API_BASE_URL_STG1_1H"]
APCA_API_KEY_ID_STG1_1H = os.environ["APCA_API_KEY_ID_STG1_1H"]
APCA_API_SECRET_KEY_STG1_1H = os.environ["APCA_API_SECRET_KEY_STG1_1H"]

APCA_API_BASE_URL_STG1_4H = os.environ["APCA_API_BASE_URL_STG1_4H"]
APCA_API_KEY_ID_STG1_4H = os.environ["APCA_API_KEY_ID_STG1_4H"]
APCA_API_SECRET_KEY_STG1_4H = os.environ["APCA_API_SECRET_KEY_STG1_4H"]


APCA_API_BASE_URL_STG2_15M = os.environ["APCA_API_BASE_URL_STG2_15M"]
APCA_API_KEY_ID_STG2_15M = os.environ["APCA_API_KEY_ID_STG2_15M"]
APCA_API_SECRET_KEY_STG2_15M = os.environ["APCA_API_SECRET_KEY_STG2_15M"]

APCA_API_BASE_URL_STG2_1H = os.environ["APCA_API_BASE_URL_STG2_1H"]
APCA_API_KEY_ID_STG2_1H = os.environ["APCA_API_KEY_ID_STG2_1H"]
APCA_API_SECRET_KEY_STG2_1H = os.environ["APCA_API_SECRET_KEY_STG2_1H"]

APCA_API_BASE_URL_STG2_4H = os.environ["APCA_API_BASE_URL_STG2_4H"]
APCA_API_KEY_ID_STG2_4H = os.environ["APCA_API_KEY_ID_STG2_4H"]
APCA_API_SECRET_KEY_STG2_4H = os.environ["APCA_API_SECRET_KEY_STG2_4H"]

POSITION_SIZE_15M = float(os.environ["POSITION_SIZE_15M"])
POSITION_SIZE_1H = float(os.environ["POSITION_SIZE_1H"])
POSITION_SIZE_4H = float(os.environ["POSITION_SIZE_4H"])


ALPACA_APIS = {
	#"real_money": tradeapi.REST(
		#base_url=APCA_API_BASE_URL_STG1_15M, 
		#key_id=APCA_API_KEY_ID_STG1_15M, 
		#secret_key=APCA_API_SECRET_KEY_STG1_15M
	#),
	"strategy1_15m_anchor": tradeapi.REST(
		base_url=APCA_API_BASE_URL_STG1_15M, 
		key_id=APCA_API_KEY_ID_STG1_15M, 
		secret_key=APCA_API_SECRET_KEY_STG1_15M
	),
	"strategy1_1h_anchor": tradeapi.REST(
		base_url=APCA_API_BASE_URL_STG1_1H, 
		key_id=APCA_API_KEY_ID_STG1_1H, 
		secret_key=APCA_API_SECRET_KEY_STG1_1H
	),
	"strategy1_4h_anchor": tradeapi.REST(
		base_url=APCA_API_BASE_URL_STG1_4H, 
		key_id=APCA_API_KEY_ID_STG1_4H, 
		secret_key=APCA_API_SECRET_KEY_STG1_4H
	),
	"strategy2_15m_anchor": tradeapi.REST(
		base_url=APCA_API_BASE_URL_STG2_15M, 
		key_id=APCA_API_KEY_ID_STG2_15M, 
		secret_key=APCA_API_SECRET_KEY_STG2_15M
	),
	"strategy2_1h_anchor": tradeapi.REST(
		base_url=APCA_API_BASE_URL_STG2_1H, 
		key_id=APCA_API_KEY_ID_STG2_1H, 
		secret_key=APCA_API_SECRET_KEY_STG2_1H
	),
	"strategy2_4h_anchor": tradeapi.REST(
		base_url=APCA_API_BASE_URL_STG2_4H, 
		key_id=APCA_API_KEY_ID_STG2_4H, 
		secret_key=APCA_API_SECRET_KEY_STG2_4H
	),	
}

MARKET_DATA_API = ALPACA_APIS["strategy1_15m_anchor"]

app = FastAPI(title="TradingView Webhook")

trading_view_webhook_helpers_instance = trading_view_webhook_helpers.TradingViewWebhookHelpers(TV_WEBHOOK_SECRET, REDIS_URL)
trade_records_instance = trade_records.TradeRecords(trading_view_webhook_helpers_instance)
strategies_instance = strategies.Strategies(trading_view_webhook_helpers_instance, trade_records_instance)
backtester_instance = backtester.BackTester(trading_view_webhook_helpers_instance, strategies_instance, trade_records_instance)
plot_instance = plot.Plot()



class SignalFlags(BaseModel):
	buy: Optional[str] = None
	buy_plus: Optional[str] = None
	sell: Optional[str] = None
	sell_plus: Optional[str] = None
	bullish_exit: Optional[str] = None
	bearish_exit: Optional[str] = None
	trend_strength: Optional[str] = None
	bar_color_value: Optional[str] = None


class TradingViewWebhook(BaseModel):
	secret: str
	symbol: str
	timeframe: str
	bar_close_time: str
	signal_role: str

	open: Optional[float] = None
	high: Optional[float] = None
	low: Optional[float] = None
	close: Optional[float] = None
	volume: Optional[float] = None

	signals: SignalFlags
 

# When app starts, this function runs once
# systemd -> docker run -> uvicorn app:app -> FastAPI app object loads -> FastAPI startup event fires -> _startup() runs
@app.on_event("startup")
def _startup():
	for strategy_name, api in ALPACA_APIS.items():
		try:
			account = api.get_account()
			if account.trading_blocked:
				logger.warning("%s account is currently restricted from trading", strategy_name)
			else:
				logger.info("%s account verified", strategy_name)
		except Exception as exc:
			logger.exception("Alpaca get_account failed during startup for %s", strategy_name)
			raise RuntimeError(f"Alpaca get_account failed during startup for {strategy_name}") from exc		


@app.get("/health")
def health():
	rr = trading_view_webhook_helpers_instance.require_redis()
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


def process_trading_signal(symbol: str, tf: str, signal: str):
	"""
	Runs strategy logic after the webhook has already been accepted.

	This keeps TradingView webhook delivery fast. Redis ingestion happens inside
	the request/response path. Alpaca calls, market-price lookup, and strategy
	execution happen here after the HTTP 200 response has been prepared.
	"""
	try:
		now_et = trading_view_webhook_helpers_instance._now_et()

		if not trading_view_webhook_helpers_instance.is_between_8pm_sun_and_8pm_fri_et(now_et):
			logger.info(
				"Strategy processing skipped outside trading window: symbol=%s tf=%s signal=%s now_et=%s",
				symbol,
				tf,
				signal,
				now_et,
			)
			return

		if not trading_view_webhook_helpers_instance.is_symbol_tradable_now(MARKET_DATA_API, symbol, now_et):
			logger.info(
				"Strategy processing skipped because symbol is not tradable now: symbol=%s tf=%s signal=%s now_et=%s",
				symbol,
				tf,
				signal,
				now_et,
			)
			return		

		prices = trade_records_instance.get_market_prices([symbol], MARKET_DATA_API)
		market_price = prices.get(symbol, {}).get("market")

		if market_price is None or market_price <= 0:
			logger.warning(
				"Strategy processing skipped due to invalid market price: symbol=%s tf=%s signal=%s market_price=%r",
				symbol,
				tf,
				signal,
				market_price,
			)
			return

		NUM_SHARES1 = POSITION_SIZE_15M / market_price
		NUM_SHARES2 = POSITION_SIZE_1H / market_price
		NUM_SHARES3 = POSITION_SIZE_4H / market_price

		##strategies_instance.entry_strategy1( # Will be implemented when we are ready to trade real money. May not be this strategy/anchor
			#"real_money",
			#"1m",
			#"5m",
			#"15m",
			#False,
			#now_et,
			#signal,
			#prices,
			#symbol,
			#tf,
			#NUM_SHARES1,
			#ALPACA_APIS["real_money"],
		#)

		#strategies_instance.exit_strategy1(
			#"real_money",
			#{"1m"},
			#"5m",
			#"15m",
			#False,
			#now_et,
			#signal,
			#prices,
			#symbol,
			#tf,
			#ALPACA_APIS["real_money"],
		#)		

		strategies_instance.exit_strategy1(
			"strategy1_15m_anchor",
			{"1m"},
			"5m",
			"15m",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			ALPACA_APIS["strategy1_15m_anchor"],
			None, None, None, None,	None,			
		)

		strategies_instance.entry_strategy1(
			"strategy1_15m_anchor",
			"1m",
			"5m",
			"15m",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			NUM_SHARES1,
			ALPACA_APIS["strategy1_15m_anchor"],
			None, None, None, None,	None,
		)

		strategies_instance.exit_strategy1(
			"strategy1_1h_anchor",
			{"1m", "5m"},
			"15m",
			"1h",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			ALPACA_APIS["strategy1_1h_anchor"],
			None, None, None, None,	None,			
		)

		strategies_instance.entry_strategy1(
			"strategy1_1h_anchor",
			"5m",
			"15m",
			"1h",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			NUM_SHARES2,
			ALPACA_APIS["strategy1_1h_anchor"],
			None, None, None, None,	None,		
		)

		strategies_instance.exit_strategy1(
			"strategy1_4h_anchor",
			{"1m", "5m", "15m"},
			"1h",
			"4h",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			ALPACA_APIS["strategy1_4h_anchor"],
			None, None, None, None,	None,			
		)

		strategies_instance.entry_strategy1(
			"strategy1_4h_anchor",
			"15m",
			"1h",
			"4h",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			NUM_SHARES3,
			ALPACA_APIS["strategy1_4h_anchor"],
			None, None, None, None,	None,		
		)

		"""
		strategies_instance.exit_strategy2(
			"strategy2_15m_anchor",
			"1m",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			ALPACA_APIS["strategy2_15m_anchor"],
			None, None, None, None,	None,
		)

		strategies_instance.entry_strategy2(
			"strategy2_15m_anchor",
			"1m",
			"5m",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			NUM_SHARES1,
			ALPACA_APIS["strategy2_15m_anchor"],
			None, None, None, None,	None,
		)

		strategies_instance.exit_strategy2(
			"strategy2_1h_anchor",
			"5m",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			ALPACA_APIS["strategy2_1h_anchor"],
			None, None, None, None,	None,
		)

		strategies_instance.entry_strategy2(
			"strategy2_1h_anchor",
			"5m",
			"15m",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			NUM_SHARES2,
			ALPACA_APIS["strategy2_1h_anchor"],
			None, None, None, None,	None,
		)

		strategies_instance.exit_strategy2(
			"strategy2_4h_anchor",
			"15m",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			ALPACA_APIS["strategy2_4h_anchor"],
			None, None, None, None,	None,
		)

		strategies_instance.entry_strategy2(
			"strategy2_4h_anchor",
			"15m",
			"1h",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			NUM_SHARES3,
			ALPACA_APIS["strategy2_4h_anchor"],
			None, None, None, None,	None,
		)
		"""

	except Exception:
		logger.exception(
			"Background strategy processing failed: symbol=%s tf=%s signal=%s",
			symbol,
			tf,
			signal,
		)


@app.post("/webhook/tradingview")
async def webhook_tradingview(payload: TradingViewWebhook, background_tasks: BackgroundTasks):
	"""
	Fast TradingView webhook handler.

	Request path:
		1. Validate secret and required fields.
		2. Acquire idempotency key.
		3. Write alert to Redis stream/state.
		4. Mark idempotency key done.
		5. Schedule strategy processing in the background.
		6. Return 200 quickly to TradingView.
	"""			
	rr = trading_view_webhook_helpers_instance.require_redis()

	if payload.secret != TV_WEBHOOK_SECRET:
		raise HTTPException(status_code=401, detail="Invalid secret")

	signals = payload.signals

	buy = trading_view_webhook_helpers_instance.safe_float(signals.buy)
	buy_plus = trading_view_webhook_helpers_instance.safe_float(signals.buy_plus)
	sell = trading_view_webhook_helpers_instance.safe_float(signals.sell)
	sell_plus = trading_view_webhook_helpers_instance.safe_float(signals.sell_plus)
	bullish_exit = trading_view_webhook_helpers_instance.safe_float(signals.bullish_exit)
	bearish_exit = trading_view_webhook_helpers_instance.safe_float(signals.bearish_exit)
	trend_strength = trading_view_webhook_helpers_instance.safe_float(signals.trend_strength)
	bar_color_value = trading_view_webhook_helpers_instance.safe_float(signals.bar_color_value)

	signal_role = str(payload.signal_role or "").strip().lower()

	signal = None

	if buy_plus == 1:
		signal = "buy+"
	elif buy == 1:
		signal = "buy"
	elif sell_plus == 1:
		signal = "sell+"
	elif sell == 1:
		signal = "sell"
	elif bullish_exit not in {None, 0.0}:
		signal = "bullish_exit"
	elif bearish_exit not in {None, 0.0}:
		signal = "bearish_exit"

	if signal is None:
		logger.warning(
			"No actionable signal detected in webhook payload: symbol=%r tf=%r signal_role=%r",
			payload.symbol,
			payload.timeframe,
			signal_role,
		)

		raise HTTPException(
			status_code=400,
			detail="No actionable signal found in payload",
		)	

	tf = trading_view_webhook_helpers_instance.normalize_tf(payload.timeframe)
	symbol = str(payload.symbol or "").upper().strip()
	bar_close_time_raw = str(payload.bar_close_time or "").strip()

	if not tf or not symbol or not signal or not bar_close_time_raw or not signal_role:
		logger.warning(
			"Invalid webhook payload: timeframe=%r symbol=%r signal=%r bar_close_time=%r signal_role=%r",
			payload.timeframe,
			payload.symbol,
			signal,
			payload.bar_close_time,
			signal_role,
		)
		raise HTTPException(
			status_code=400,
			detail="Missing/invalid timeframe, symbol, signal, or bar close time",
		)

	acquired, dedupe_key = trading_view_webhook_helpers_instance.acquire_alert_idempotency(
		symbol=symbol,
		timeframe=tf,
		signal=signal,
		bar_close_time=bar_close_time_raw,
		signal_role=signal_role
	)

	if not acquired:
		existing = rr.get(dedupe_key)

		if existing == "processing":
			logger.info(
				"TradingView alert already in progress: symbol=%s tf=%s signal=%s bar_close_time=%s signal_role=%s dedupe_key=%s",
				symbol,
				tf,
				signal,
				bar_close_time_raw,
				signal_role,
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
				"signal_role": signal_role
			}

		if existing and existing.startswith("done:"):
			existing_stream_id = existing.split("done:", 1)[1]
			logger.info(
				"Duplicate TradingView alert ignored: symbol=%s tf=%s signal=%s bar_close_time=%s signal_role=%s dedupe_key=%s stream_id=%s",
				symbol,
				tf,
				signal,
				bar_close_time_raw,
				signal_role,
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
				"signal_role": signal_role,
				"stream_id": existing_stream_id,
			}

		logger.info(
			"Duplicate TradingView alert ignored with unexpected dedupe state: symbol=%s tf=%s signal=%s bar_close_time=%s signal_role=%s dedupe_key=%s value=%r",
			symbol,
			tf,
			signal,
			bar_close_time_raw,
			signal_role,
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
			"signal_role": signal_role,
		}

	stream_key = trading_view_webhook_helpers_instance.stream_key(tf, symbol)
	state_key = trading_view_webhook_helpers_instance.state_key(tf, symbol)

	received_at = trading_view_webhook_helpers_instance.utc_now_iso()
	bar_close_time_eastern = trading_view_webhook_helpers_instance.parse_iso_to_eastern(bar_close_time_raw)

	stream_fields = {
		"symbol": symbol,
		"timeframe": tf,
		"signal": signal,
		"bar_close_time_eastern": trading_view_webhook_helpers_instance.to_str(bar_close_time_eastern),
		"trend_strength": trading_view_webhook_helpers_instance.to_str(trend_strength),
		"bar_color_value": trading_view_webhook_helpers_instance.to_str(bar_color_value),
		"signal_role": trading_view_webhook_helpers_instance.to_str(signal_role),
		"received_at": received_at,
		"open": trading_view_webhook_helpers_instance.to_str(payload.open),
		"high": trading_view_webhook_helpers_instance.to_str(payload.high),
		"low": trading_view_webhook_helpers_instance.to_str(payload.low),
		"close": trading_view_webhook_helpers_instance.to_str(payload.close),
		"volume": trading_view_webhook_helpers_instance.to_str(payload.volume),
	}

	state_fields = {
		"symbol": symbol,
		"timeframe": tf,
		"signal": signal,
		"bar_close_time_eastern": trading_view_webhook_helpers_instance.to_str(bar_close_time_eastern),
		"trend_strength": trading_view_webhook_helpers_instance.to_str(trend_strength),
		"bar_color_value": trading_view_webhook_helpers_instance.to_str(bar_color_value),
		"signal_role": trading_view_webhook_helpers_instance.to_str(signal_role),
		"received_at": received_at,
		"open": trading_view_webhook_helpers_instance.to_str(payload.open),
		"high": trading_view_webhook_helpers_instance.to_str(payload.high),
		"low": trading_view_webhook_helpers_instance.to_str(payload.low),
		"close": trading_view_webhook_helpers_instance.to_str(payload.close),
		"volume": trading_view_webhook_helpers_instance.to_str(payload.volume),
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

		rr.set(
			dedupe_key,
			f"done:{stream_id}",
			xx=True,
			ex=trading_view_webhook_helpers_instance.alert_dedupe_ttl_seconds,
		)

	except Exception:
		try:
			rr.delete(dedupe_key)
		except Exception:
			logger.exception("Failed to clear idempotency key after processing failure")

		logger.exception("Redis write failed")
		raise HTTPException(status_code=500, detail="Redis write failed")

	logger.info(
		"\n{\n[TV] recv_utc=%s\nsymbol=%s\ntf=%s\nsignal=%s\nbar_close_time_eastern=%s\ntrend_strength=%s\nbar_color_value=%s\nsignal_role=%s\nopen=%s\nhigh=%s\nlow=%s\nclose=%s\nvolume=%s\n}\n",
		received_at,
		symbol,
		tf,
		str(signal).upper(),
		bar_close_time_eastern,
		trend_strength,
		bar_color_value,
		signal_role,
		payload.open,
		payload.high,
		payload.low,
		payload.close,
		payload.volume,
	)

	background_tasks.add_task(
		process_trading_signal,
		symbol,
		tf,
		signal,
	)

	return {
		"ok": True,
		"accepted": True,
		"symbol": symbol,
		"timeframe": tf,
		"signal": signal,
		"trend_strength":trading_view_webhook_helpers_instance.to_str(trend_strength),
		"bar_color_value":trading_view_webhook_helpers_instance.to_str(bar_color_value),
		"signal_role": trading_view_webhook_helpers_instance.to_str(signal_role),
		"stream": stream_key,
		"state": state_key,
		"stream_id": stream_id,
		"maxlen": TV_MAXLEN,
	}
	


@app.get("/backtest/run")
def run_backtest(
	strategy_name: str = Query(..., min_length=1),
	start: str = Query(..., min_length=1),
	end: str = Query(..., min_length=1),
	tickers: Optional[str] = Query(default=None, description="Optional comma-separated ticker list"),
	position_size: Optional[float] = Query(default=None, gt=0),
	ATR_period: int = Query(default=14, ge=1),
):
	"""
	Run an isolated Redis-signal backtest and return JSON results.

	This endpoint reads historical TradingView signal streams from Redis, simulates
	strategy decisions in chronological order, keeps positions/PnL/exposure in memory,
	prints the simulated daily max exposure table, and does not touch live Alpaca or
	live Redis trade/PnL/position state.

	Example:
		curl "http://localhost:8000/backtest/run?strategy_name=strategy1_15m_anchor&start=2026-06-01T04:00:00-04:00&end=2026-06-01T20:00:00-04:00&position_size=5000"
	Or
		curl -s "http://localhost:8000/backtest/run?strategy_name=strategy1_15m_anchor&start=2026-06-01T04:00:00-04:00&end=2026-06-01T20:00:00-04:00&position_size=5000" \
> backtest.json	
	"""
	try:
		ticker_list = [item.strip() for item in tickers.split(",")] if tickers else None
		return backtester_instance.run(
			alpaca_api=MARKET_DATA_API,
			strategy_name=strategy_name,
			start=start,
			end=end,
			tickers=ticker_list,
			position_size=position_size,
			ATR_period=ATR_period,
		)
	except ValueError as exc:
		raise HTTPException(status_code=400, detail=str(exc))
	except Exception:
		logger.exception("Backtest failed")
		raise HTTPException(status_code=500, detail="Backtest failed")


@app.get("/backtest/plot")
def plot_backtest(
	strategy_name: str = Query(..., min_length=1),
	start: str = Query(..., min_length=1),
	end: str = Query(..., min_length=1),
	tickers: Optional[str] = Query(
		default=None,
		description="Optional comma-separated ticker list",
	),
	position_size: Optional[float] = Query(default=None, gt=0),
	ATR_period: int = Query(default=14, ge=1),
):
	"""
	Run an isolated Redis-signal backtest and stream separate chart PNGs as a ZIP archive.

	The simulation is recomputed in memory for this request. It reads Redis signal
	streams only and does not write simulated positions, trade events, PnL, or exposure
	into the live Redis keys.

	Example:
		In laptop

			ssh -i ~/.ssh/my-aws-ec2-key.pem ubuntu@54.176.151.9 \
			'curl -sS --fail "http://localhost:8000/backtest/plot?strategy_name=strategy1_15m_anchor&start=2026-06-01T04:00:00-04:00&end=2026-06-01T20:00:00-04:00&position_size=5000"' \
			> backtest_charts.zip

			rm -rf backtest_charts
			mkdir backtest_charts
			unzip -q backtest_charts.zip -d backtest_charts
			python show_backtest_charts.py	
	"""
	try:
		ticker_list = (
			[item.strip() for item in tickers.split(",")]
			if tickers
			else None
		)

		result = backtester_instance.run(
			alpaca_api=MARKET_DATA_API,
			strategy_name=strategy_name,
			start=start,
			end=end,
			tickers=ticker_list,
			position_size=position_size,
			ATR_period=ATR_period,
		)

		#image_buffer = backtester_instance.plot_overall_pnl(result)
		#return StreamingResponse(image_buffer, media_type="image/png")

		zip_buffer = backtester_instance.build_backtest_chart_zip(
			result
		)

		return StreamingResponse(
			zip_buffer,
			media_type="application/zip",
			headers={
				"Content-Disposition": (
					'attachment; filename="backtest_charts.zip"'
				),
			},
		)

	except ValueError as exc:
		raise HTTPException(status_code=400, detail=str(exc))
	except Exception:
		logger.exception("Backtest plot failed")
		raise HTTPException(status_code=500, detail="Backtest plot failed")

