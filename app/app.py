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
import threading
import math


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

#APCA_API_BASE_URL_STG1_1H = os.environ["APCA_API_BASE_URL_STG1_1H"]
#APCA_API_KEY_ID_STG1_1H = os.environ["APCA_API_KEY_ID_STG1_1H"]
#APCA_API_SECRET_KEY_STG1_1H = os.environ["APCA_API_SECRET_KEY_STG1_1H"]

#APCA_API_BASE_URL_STG1_4H = os.environ["APCA_API_BASE_URL_STG1_4H"]
#APCA_API_KEY_ID_STG1_4H = os.environ["APCA_API_KEY_ID_STG1_4H"]
#APCA_API_SECRET_KEY_STG1_4H = os.environ["APCA_API_SECRET_KEY_STG1_4H"]

#APCA_API_BASE_URL_STG2_15M = os.environ["APCA_API_BASE_URL_STG2_15M"]
#APCA_API_KEY_ID_STG2_15M = os.environ["APCA_API_KEY_ID_STG2_15M"]
#APCA_API_SECRET_KEY_STG2_15M = os.environ["APCA_API_SECRET_KEY_STG2_15M"]

#APCA_API_BASE_URL_STG2_1H = os.environ["APCA_API_BASE_URL_STG2_1H"]
#APCA_API_KEY_ID_STG2_1H = os.environ["APCA_API_KEY_ID_STG2_1H"]
#APCA_API_SECRET_KEY_STG2_1H = os.environ["APCA_API_SECRET_KEY_STG2_1H"]

#APCA_API_BASE_URL_STG2_4H = os.environ["APCA_API_BASE_URL_STG2_4H"]
#APCA_API_KEY_ID_STG2_4H = os.environ["APCA_API_KEY_ID_STG2_4H"]
#APCA_API_SECRET_KEY_STG2_4H = os.environ["APCA_API_SECRET_KEY_STG2_4H"]

#APCA_API_BASE_URL_STG4_15M = os.environ["APCA_API_BASE_URL_STG4_15M"]
#APCA_API_KEY_ID_STG4_15M = os.environ["APCA_API_KEY_ID_STG4_15M"]
#APCA_API_SECRET_KEY_STG4_15M = os.environ["APCA_API_SECRET_KEY_STG4_15M"]

APCA_API_BASE_URL_STG4_1H = os.environ["APCA_API_BASE_URL_STG4_1H"]
APCA_API_KEY_ID_STG4_1H = os.environ["APCA_API_KEY_ID_STG4_1H"]
APCA_API_SECRET_KEY_STG4_1H = os.environ["APCA_API_SECRET_KEY_STG4_1H"]

APCA_API_BASE_URL_STG4B_1H = os.environ["APCA_API_BASE_URL_STG4B_1H"]
APCA_API_KEY_ID_STG4B_1H = os.environ["APCA_API_KEY_ID_STG4B_1H"]
APCA_API_SECRET_KEY_STG4B_1H = os.environ["APCA_API_SECRET_KEY_STG4B_1H"]

#APCA_API_BASE_URL_STG4_4H = os.environ["APCA_API_BASE_URL_STG4_4H"]
#APCA_API_KEY_ID_STG4_4H = os.environ["APCA_API_KEY_ID_STG4_4H"]
#APCA_API_SECRET_KEY_STG4_4H = os.environ["APCA_API_SECRET_KEY_STG4_4H"]


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
	#"strategy1_1h_anchor": tradeapi.REST(
		#base_url=APCA_API_BASE_URL_STG1_1H, 
		#key_id=APCA_API_KEY_ID_STG1_1H, 
		#secret_key=APCA_API_SECRET_KEY_STG1_1H
	#),
	#"strategy1_4h_anchor": tradeapi.REST(
		#base_url=APCA_API_BASE_URL_STG1_4H, 
		#key_id=APCA_API_KEY_ID_STG1_4H, 
		#secret_key=APCA_API_SECRET_KEY_STG1_4H
	#),
	#"strategy2_15m_anchor": tradeapi.REST(
		#base_url=APCA_API_BASE_URL_STG2_15M, 
		#key_id=APCA_API_KEY_ID_STG2_15M, 
		#secret_key=APCA_API_SECRET_KEY_STG2_15M
	#),
	#"strategy2_1h_anchor": tradeapi.REST(
		#base_url=APCA_API_BASE_URL_STG2_1H, 
		#key_id=APCA_API_KEY_ID_STG2_1H, 
		#secret_key=APCA_API_SECRET_KEY_STG2_1H
	#),
	#"strategy2_4h_anchor": tradeapi.REST(
		#base_url=APCA_API_BASE_URL_STG2_4H, 
		#key_id=APCA_API_KEY_ID_STG2_4H, 
		#secret_key=APCA_API_SECRET_KEY_STG2_4H
	#),	
	#"strategy4_15m_anchor": tradeapi.REST(
		#base_url=APCA_API_BASE_URL_STG4_15M,
		#key_id=APCA_API_KEY_ID_STG4_15M,
		#secret_key=APCA_API_SECRET_KEY_STG4_15M,
	#),
	"strategy4_1h_anchor": tradeapi.REST(	
		base_url=APCA_API_BASE_URL_STG4_1H,
		key_id=APCA_API_KEY_ID_STG4_1H,
		secret_key=APCA_API_SECRET_KEY_STG4_1H,
	),
	"strategy4b_1h_anchor": tradeapi.REST(	
		base_url=APCA_API_BASE_URL_STG4B_1H,
		key_id=APCA_API_KEY_ID_STG4B_1H,
		secret_key=APCA_API_SECRET_KEY_STG4B_1H,
	),	
	#"strategy4_4h_anchor": tradeapi.REST(
		#base_url=APCA_API_BASE_URL_STG4_4H,
		#key_id=APCA_API_KEY_ID_STG4_4H,
		#secret_key=APCA_API_SECRET_KEY_STG4_4H,
	#),	
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
 


def env_bool(
	name: str,
	default: bool = False,
) -> bool:
	raw_value = os.getenv(
		name
	)

	if raw_value is None:
		return default

	value = str(
		raw_value
	).strip().lower()

	if value in {
		"1",
		"true",
		"yes",
		"on",
	}:
		return True

	if value in {
		"0",
		"false",
		"no",
		"off",
	}:
		return False

	raise RuntimeError(
		f"{name} must be true or false"
	)


TRAILING_STOP_EXIT_ENABLED = env_bool(
	"TRAILING_STOP_EXIT_ENABLED",
	False,
)

TRAILING_STOP_EXIT_ATR_PERIOD = int(
	os.getenv(
		"TRAILING_STOP_EXIT_ATR_PERIOD",
		"14",
	)
)

TRAILING_STOP_EXIT_ATR_MULTIPLIER = float(
	os.getenv(
		"TRAILING_STOP_EXIT_ATR_MULTIPLIER",
		"0.8",
	)
)

TRAILING_STOP_EXIT_LOSS_LIQUIDATION_ATR_FACTOR = float(
	os.getenv(
		"TRAILING_STOP_EXIT_LOSS_LIQUIDATION_ATR_FACTOR",
		"0.6",
	)
)

TRAILING_STOP_EXIT_PROFIT_EXPANSION_ATR_FACTOR = float(
	os.getenv(
		"TRAILING_STOP_EXIT_PROFIT_EXPANSION_ATR_FACTOR",
		"2.5",
	)
)

TRAILING_STOP_EXIT_MULTIPLIER_FACTOR = float(
	os.getenv(
		"TRAILING_STOP_EXIT_MULTIPLIER_FACTOR",
		"1.8",
	)
)

TRAILING_STOP_EXIT_LIQUIDATE_BEFORE_MARKET_CLOSE = env_bool(
	"TRAILING_STOP_EXIT_LIQUIDATE_BEFORE_MARKET_CLOSE",
	True,
)

TRAILING_STOP_EXIT_MARKET_CLOSE_BUFFER_SECONDS = int(
	os.getenv(
		"TRAILING_STOP_EXIT_MARKET_CLOSE_BUFFER_SECONDS",
		"300",
	)
)

TRAILING_STOP_EXIT_MONITOR_INTERVAL_SECONDS = int(
	os.getenv(
		"TRAILING_STOP_EXIT_MONITOR_INTERVAL_SECONDS",
		"30",
	)
)

TRAILING_STOP_EXIT_MANAGER_LOCK_SECONDS = int(
	os.getenv(
		"TRAILING_STOP_EXIT_MANAGER_LOCK_SECONDS",
		"20",
	)
)


if TRAILING_STOP_EXIT_ATR_PERIOD < 1:
	raise RuntimeError(
		"TRAILING_STOP_EXIT_ATR_PERIOD must be >= 1"
	)

if TRAILING_STOP_EXIT_ATR_MULTIPLIER <= 0:
	raise RuntimeError(
		"TRAILING_STOP_EXIT_ATR_MULTIPLIER must be > 0"
	)

if (
	TRAILING_STOP_EXIT_LOSS_LIQUIDATION_ATR_FACTOR
	<= 0
):
	raise RuntimeError(
		"TRAILING_STOP_EXIT_LOSS_LIQUIDATION_ATR_FACTOR "
		"must be > 0"
	)

if (
	TRAILING_STOP_EXIT_PROFIT_EXPANSION_ATR_FACTOR
	<= 0
):
	raise RuntimeError(
		"TRAILING_STOP_EXIT_PROFIT_EXPANSION_ATR_FACTOR "
		"must be > 0"
	)

if TRAILING_STOP_EXIT_MULTIPLIER_FACTOR < 1:
	raise RuntimeError(
		"TRAILING_STOP_EXIT_MULTIPLIER_FACTOR must be >= 1"
	)

if TRAILING_STOP_EXIT_MONITOR_INTERVAL_SECONDS < 1:
	raise RuntimeError(
		"TRAILING_STOP_EXIT_MONITOR_INTERVAL_SECONDS "
		"must be >= 1"
	)	

if TRAILING_STOP_EXIT_MARKET_CLOSE_BUFFER_SECONDS < 1:
	raise RuntimeError(
		"TRAILING_STOP_EXIT_MARKET_CLOSE_BUFFER_SECONDS "
		"must be >= 1"
	)

if TRAILING_STOP_EXIT_MANAGER_LOCK_SECONDS < 1:
	raise RuntimeError(
		"TRAILING_STOP_EXIT_MANAGER_LOCK_SECONDS "
		"must be >= 1"
	)	

LIVE_TRAILING_STOP_ACCOUNTS = {
	"strategy4_1h_anchor": {
		"enabled": True,
		"alpaca_api": ALPACA_APIS[
			"strategy4_1h_anchor"
		],
		"use_trailing_stop": True,
		"use_profit_expansion": True,		
	},
	"strategy4b_1h_anchor": {
		"enabled": True,
		"alpaca_api": ALPACA_APIS[
			"strategy4b_1h_anchor"
		],
		"use_trailing_stop": False,
		"use_profit_expansion": False,		
	},	
}


trailing_stop_exit_stop_event = threading.Event()
trailing_stop_exit_wake_event = threading.Event()
trailing_stop_exit_thread = None


def run_live_trailing_stop_exit_monitor() -> None:
	logger.info(
		"Live trailing-stop exit monitor started"
	)

	interval_seconds = (
		TRAILING_STOP_EXIT_MONITOR_INTERVAL_SECONDS
	)

	while not trailing_stop_exit_stop_event.is_set():
		trailing_stop_exit_wake_event.wait(
			timeout=interval_seconds
		)
		trailing_stop_exit_wake_event.clear()

		if trailing_stop_exit_stop_event.is_set():
			break

		for owner_name, account_config in (
			LIVE_TRAILING_STOP_ACCOUNTS.items()
		):
			if not account_config.get(
				"enabled",
				False,
			):
				continue

			try:
				strategies_instance.manage_live_positions(
					owner_name=owner_name,
					alpaca_api=account_config[
						"alpaca_api"
					],
					atr_period=(
						TRAILING_STOP_EXIT_ATR_PERIOD
					),
					atr_multiplier=(
						TRAILING_STOP_EXIT_ATR_MULTIPLIER
					),
					loss_liquidation_atr_factor=(
						TRAILING_STOP_EXIT_LOSS_LIQUIDATION_ATR_FACTOR
					),
					profit_expansion_atr_factor=(
						TRAILING_STOP_EXIT_PROFIT_EXPANSION_ATR_FACTOR
					),
					trailing_stop_multiplier_factor=(
						TRAILING_STOP_EXIT_MULTIPLIER_FACTOR
					),
					use_trailing_stop=account_config[
						"use_trailing_stop"
					],
					use_profit_expansion=account_config[
						"use_profit_expansion"
					],					
					liquidate_before_market_close=(
						TRAILING_STOP_EXIT_LIQUIDATE_BEFORE_MARKET_CLOSE
					),
					market_close_buffer_seconds=(
						TRAILING_STOP_EXIT_MARKET_CLOSE_BUFFER_SECONDS
					),
					manager_lock_seconds=(
						TRAILING_STOP_EXIT_MANAGER_LOCK_SECONDS
					),
				)

			except Exception:
				logger.exception(
					"Live trailing-stop monitor iteration "
					"failed: owner=%s",
					owner_name,
				)

	logger.info(
		"Live trailing-stop exit monitor stopped"
	)


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

	global trailing_stop_exit_thread

	if TRAILING_STOP_EXIT_ENABLED:
		trailing_stop_exit_thread = threading.Thread(
			target=run_live_trailing_stop_exit_monitor,
			name="live-trailing-stop-exit-monitor",
			daemon=True,
		)

		trailing_stop_exit_thread.start()


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


@app.on_event("shutdown")
def _shutdown():
	trailing_stop_exit_stop_event.set()
	trailing_stop_exit_wake_event.set()	

	if (
		trailing_stop_exit_thread is not None
		and trailing_stop_exit_thread.is_alive()
	):
		trailing_stop_exit_thread.join(
			timeout=10
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
		NUM_SHARES2 = math.floor(POSITION_SIZE_1H / market_price) #Since the strategy tied to this number of shares will use trailing stops
		NUM_SHARES3 = POSITION_SIZE_4H / market_price

		if NUM_SHARES2 < 1:
			logger.info(
				"Strategy 4 entry skipped because position size "
				"is insufficient for one whole share: "
				"ticker=%r position_size=%r market_price=%r",
				symbol,
				POSITION_SIZE_1H,
				market_price,
			)
			return		

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

		#strategies_instance.exit_strategy1(
			#"strategy1_1h_anchor",
			#{"1m", "5m"},
			#"15m",
			#"1h",
			#False,
			#now_et,
			#signal,
			#prices,
			#symbol,
			#tf,
			#ALPACA_APIS["strategy1_1h_anchor"],
			#None, None, None, None,	None,			
		#)

		#strategies_instance.entry_strategy1(
			#"strategy1_1h_anchor",
			#"5m",
			#"15m",
			#"1h",
			#False,
			#now_et,
			#signal,
			#prices,
			#symbol,
			#tf,
			#NUM_SHARES2,
			#ALPACA_APIS["strategy1_1h_anchor"],
			#None, None, None, None,	None,		
		#)

		entry_is_blocked = (
			strategies_instance
			.live_trailing_stop_entry_is_blocked(
				owner_name="strategy4_1h_anchor",
				ticker=symbol,
				alpaca_api=ALPACA_APIS[
					"strategy4_1h_anchor"
				],
			)
		)

		entries_closed_for_day = (
			strategies_instance
			.live_trailing_stop_entries_closed_for_day(
				owner_name="strategy4_1h_anchor",
				now_et=now_et,
			)
		)	

		if entries_closed_for_day:
			logger.info(
				"Strategy 4 entry skipped because trailing-stop "
				"entries are closed for the day: "
				"owner=%r ticker=%r now_et=%s",
				"strategy4_1h_anchor",
				symbol,
				now_et,
			)			

		if (entry_is_blocked or entries_closed_for_day):
			submitted_order = None
		else:
			submitted_order = (
				strategies_instance.entry_strategy4(
					"strategy4_1h_anchor",
					"1h",
					False,
					now_et,
					signal,
					prices,
					symbol,
					tf,
					NUM_SHARES2,
					ALPACA_APIS["strategy4_1h_anchor"],
					None,
					None,
					None,
					None,
					None,
				)
			)

			if submitted_order is not None:
				entry_order_id = str(
					submitted_order.get(
						"order_id",
						"",
					)
					or ""
				).strip()

				if not entry_order_id:
					logger.error(
						"Strategy 4 entry returned no order ID; "
						"trailing-stop registration skipped: "
						"owner=%r ticker=%r submitted_order=%r",
						"strategy4_1h_anchor",
						symbol,
						submitted_order,
					)

				else:
					strategies_instance.register_live_position(
						owner_name="strategy4_1h_anchor",
						ticker=symbol,
						anchor_tf="1h",
						entry_order_id=entry_order_id,
						entry_decision_time=now_et,
					)

					trailing_stop_exit_wake_event.set()





		entry_is_blocked_b = (
			strategies_instance
			.live_trailing_stop_entry_is_blocked(
				owner_name="strategy4b_1h_anchor",
				ticker=symbol,
				alpaca_api=ALPACA_APIS[
					"strategy4b_1h_anchor"
				],
			)
		)
		entries_closed_for_day_b = (
			strategies_instance
			.live_trailing_stop_entries_closed_for_day(
				owner_name="strategy4b_1h_anchor",
				now_et=now_et,
			)
		)	

		if entries_closed_for_day_b:
			logger.info(
				"Strategy 4b entry skipped because trailing-stop "
				"entries are closed for the day: "
				"owner=%r ticker=%r now_et=%s",
				"strategy4b_1h_anchor",
				symbol,
				now_et,
			)
		strategies_instance.exit_strategy4(
			"strategy4b_1h_anchor",
			"1h",
			TRAILING_STOP_EXIT_LOSS_LIQUIDATION_ATR_FACTOR,
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			ALPACA_APIS["strategy4b_1h_anchor"],
			None,None,None,None,None,
		)

		if (entry_is_blocked_b or entries_closed_for_day_b):
			submitted_order_b = None
		else:
			submitted_order_b = (
				strategies_instance.entry_strategy4(
					"strategy4b_1h_anchor",
					"1h",
					False,
					now_et,
					signal,
					prices,
					symbol,
					tf,
					NUM_SHARES2,
					ALPACA_APIS["strategy4b_1h_anchor"],
					None,None,None,None,None,
				)
			)

		"""entry_is_blocked_b = (
			strategies_instance
			.live_trailing_stop_entry_is_blocked(
				owner_name="strategy4b_1h_anchor",
				ticker=symbol,
				alpaca_api=ALPACA_APIS[
					"strategy4b_1h_anchor"
				],
			)
		)

		entries_closed_for_day_b = (
			strategies_instance
			.live_trailing_stop_entries_closed_for_day(
				owner_name="strategy4b_1h_anchor",
				now_et=now_et,
			)
		)	

		if entries_closed_for_day_b:
			logger.info(
				"Strategy 4b entry skipped because trailing-stop "
				"entries are closed for the day: "
				"owner=%r ticker=%r now_et=%s",
				"strategy4b_1h_anchor",
				symbol,
				now_et,
			)			

		if (entry_is_blocked_b or entries_closed_for_day_b):
			submitted_order_b = None
		else:
			strategies_instance.exit_strategy4(
				"strategy4b_1h_anchor",
				"1h",
				TRAILING_STOP_EXIT_LOSS_LIQUIDATION_ATR_FACTOR,
				False,
				now_et,
				signal,
				prices,
				symbol,
				tf,
				ALPACA_APIS["strategy4b_1h_anchor"],
				None, None, None, None, None,
			)

			submitted_order_b = strategies_instance.entry_strategy4(
				"strategy4b_1h_anchor",
				"1h",
				False,
				now_et,
				signal,
				prices,
				symbol,
				tf,
				NUM_SHARES2,
				ALPACA_APIS["strategy4b_1h_anchor"],
				None, None, None, None, None,
			)

			if submitted_order_b is not None:
				entry_order_id_b = str(
					submitted_order_b.get(
						"order_id",
						"",
					)
					or ""
				).strip()

				if entry_order_id_b:
					strategies_instance.register_live_position(
						owner_name="strategy4b_1h_anchor",
						ticker=symbol,
						anchor_tf="1h",
						entry_order_id=entry_order_id_b,
						entry_decision_time=now_et,
					)

					trailing_stop_exit_wake_event.set()"""


		#strategies_instance.exit_strategy1(
			#"strategy1_4h_anchor",
			#{"1m", "5m", "15m"},
			#"1h",
			#"4h",
			#False,
			#now_et,
			#signal,
			#prices,
			#symbol,
			#tf,
			#ALPACA_APIS["strategy1_4h_anchor"],
			#None, None, None, None,	None,			
		#)

		#strategies_instance.entry_strategy1(
			#"strategy1_4h_anchor",
			#"15m",
			#"1h",
			#"4h",
			#False,
			#now_et,
			#signal,
			#prices,
			#symbol,
			#tf,
			#NUM_SHARES3,
			#ALPACA_APIS["strategy1_4h_anchor"],
			#None, None, None, None,	None,		
		#)

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


		strategies_instance.exit_strategy4(
			"strategy4_15m_anchor",
			"15m",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			ALPACA_APIS["strategy4_15m_anchor"],
			None,
			None,
			None,
			None,
			None,
		)

		strategies_instance.entry_strategy4(
			"strategy4_15m_anchor",
			"15m",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			NUM_SHARES1,
			ALPACA_APIS["strategy4_15m_anchor"],
			None,
			None,
			None,
			None,
			None,
		)

		strategies_instance.exit_strategy4(
			"strategy4_1h_anchor",
			"1h",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			ALPACA_APIS["strategy4_1h_anchor"],
			None,
			None,
			None,
			None,
			None,
		)

		strategies_instance.entry_strategy4(
			"strategy4_1h_anchor",
			"1h",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			NUM_SHARES2,
			ALPACA_APIS["strategy4_1h_anchor"],
			None,
			None,
			None,
			None,
			None,
		)

		strategies_instance.exit_strategy4(
			"strategy4_4h_anchor",
			"4h",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			ALPACA_APIS["strategy4_4h_anchor"],
			None,
			None,
			None,
			None,
			None,
		)

		strategies_instance.entry_strategy4(
			"strategy4_4h_anchor",
			"4h",
			False,
			now_et,
			signal,
			prices,
			symbol,
			tf,
			NUM_SHARES3,
			ALPACA_APIS["strategy4_4h_anchor"],
			None,
			None,
			None,
			None,
			None,
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
	ATR_multiplier: float = Query(
		default=1.0,
		gt=0,
	),	
	loss_liquidation_atr_factor: Optional[float] = Query(
		default=None,
		gt=0,
		description=(
			"For supported exit strategies, liquidate a long "
			"when price falls below cost basis by this factor "
			"multiplied by entry ATR. Mirror the condition for shorts."
		),
	),
	profit_expansion_atr_factor: Optional[float] = Query(
		default=None,
		gt=0,
		description=(
			"For Exit Strategy 3, widen the trailing stop once profit "
			"reaches this factor multiplied by entry ATR."
		),
	),
	trailing_stop_multiplier_factor: float = Query(
		default=1.0,
		ge=1.0,
		description=(
			"Factor multiplied by the original ATR_multiplier after the "
			"profit-expansion threshold is reached."
		),
	),	
	exit_strategy: Optional[int] = Query(
		default=None,
		ge=1,
		le=4,
	),	
	liquidate_before_market_close: bool = Query(
		default=False,
		description=(
			"Liquidate all open positions one minute "
			"before the official market close"
		),
	),
	run_exit_strategy: bool = Query(
		default=True,
		description=(
			"Run the configured exit strategy. When false, "
			"only validate and record entry conditions without "
			"opening or modifying simulated positions."
		),
	),
	record_factor_research: bool = Query(
		default=False,
		description=(
			"Persist one completed-trade research row for "
			"later liquidation-factor analysis"
		),
	),
	research_group_id: Optional[str] = Query(
		default=None,
		description=(
			"Stable identifier shared by every liquidation-factor "
			"run in the same research experiment"
		),
	),					
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
			ATR_multiplier=ATR_multiplier,
			loss_liquidation_atr_factor=(loss_liquidation_atr_factor),
			profit_expansion_atr_factor=(profit_expansion_atr_factor),
			trailing_stop_multiplier_factor=(trailing_stop_multiplier_factor),			
			exit_strategy=exit_strategy,
			liquidate_before_market_close=liquidate_before_market_close,
			run_exit_strategy=run_exit_strategy,
			record_factor_research=record_factor_research,
			research_group_id=research_group_id,			
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
	ATR_multiplier: float = Query(
		default=1.0,
		gt=0,
	),	
	loss_liquidation_atr_factor: Optional[float] = Query(
		default=None,
		gt=0,
		description=(
			"For supported exit strategies, liquidate a long "
			"when price falls below cost basis by this factor "
			"multiplied by entry ATR. Mirror the condition for shorts."
		),
	),
	profit_expansion_atr_factor: Optional[float] = Query(
		default=None,
		gt=0,
		description=(
			"For Exit Strategy 3, widen the trailing stop once profit "
			"reaches this factor multiplied by entry ATR."
		),
	),
	trailing_stop_multiplier_factor: float = Query(
		default=1.0,
		ge=1.0,
		description=(
			"Factor multiplied by the original ATR_multiplier after the "
			"profit-expansion threshold is reached."
		),
	),	
	exit_strategy: Optional[int] = Query(
		default=None,
		ge=1,
		le=4,
	),	
	liquidate_before_market_close: bool = Query(
		default=False,
		description=(
			"Liquidate all open positions one minute "
			"before the official market close"
		),
	),	
	run_exit_strategy: bool = Query(
		default=True,
		description=(
			"Run the configured exit strategy. When false, "
			"only validate and record entry conditions without "
			"opening or modifying simulated positions."
		),
	),	
	record_factor_research: bool = Query(
		default=False,
		description=(
			"Persist one completed-trade research row for "
			"later liquidation-factor analysis"
		),
	),
	research_group_id: Optional[str] = Query(
		default=None,
		description=(
			"Stable identifier shared by every liquidation-factor "
			"run in the same research experiment"
		),
	),		
):
	"""
	Run an isolated Redis-signal backtest and stream separate chart PNGs as a ZIP archive.

	The simulation is recomputed in memory for this request. It reads Redis signal
	streams only and does not write simulated positions, trade events, PnL, or exposure
	into the live Redis keys.

	Example:
		In laptop

			cd ~/Documents/GitHub/TradingMage
			source .venv/bin/activate		

			ssh -i ~/.ssh/my-aws-ec2-key ubuntu@54.176.151.9 \
			'curl -sS --fail "http://localhost:8000/backtest/plot?strategy_name=strategy1_15m_anchor&start=2026-06-01T04:00:00-04:00&end=2026-06-01T20:00:00-04:00&position_size=5000"' \
			> backtest_charts.zip

			rm -rf backtest_charts
			mkdir backtest_charts
			unzip -q backtest_charts.zip -d backtest_charts
			python3 show_backtest_charts.py	
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
			ATR_multiplier=ATR_multiplier,
			loss_liquidation_atr_factor=(loss_liquidation_atr_factor),
			profit_expansion_atr_factor=(profit_expansion_atr_factor),
			trailing_stop_multiplier_factor=(trailing_stop_multiplier_factor),				
			exit_strategy=exit_strategy,
			liquidate_before_market_close=liquidate_before_market_close,
			run_exit_strategy=run_exit_strategy,
			record_factor_research=record_factor_research,
			research_group_id=research_group_id,
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



@app.get("/backtest/factor-research/plot")
def plot_backtest_factor_research(
	research_group_id: str = Query(
		...,
		min_length=1,
	),
	minimum_pnl_margin: float = Query(
		default=0.0,
		ge=0.0,
		description=(
			"Minimum percentage-point PnL advantage required "
			"for a winning liquidation factor"
		),
	),
	pnl_tie_tolerance: float = Query(
		default=0.0,
		ge=0.0,
		description=(
			"Treat factor results within this many PnL percentage "
			"points as tied and choose the smaller factor"
		),
	),
	require_all_factors: bool = Query(
		default=True,
		description=(
			"Include only trades found under every liquidation "
			"factor in the research group"
		),
	),
):
	"""
	E.g.
	RESEARCH_GROUP="strategy4_1h_20260531_20260804_v1"
	echo "$RESEARCH_GROUP"
	ssh -i ~/.ssh/my-aws-ec2-key ubuntu@54.176.151.9 \
	'curl -sS --fail-with-body "http://localhost:8000/backtest/run?strategy_name=strategy4_1h_anchor&start=2026-05-31T04:00:00-04:00&end=2026-08-04T20:00:00-04:00&position_size=6600&exit_strategy=4&loss_liquidation_atr_factor=0.6&liquidate_before_market_close=true&record_factor_research=true&research_group_id=strategy4_1h_20260531_20260804_v1"' \
	> factor_0.6.json	
	Repeat for other factors
	Then
	ssh -i ~/.ssh/my-aws-ec2-key ubuntu@54.176.151.9 \
	'curl -sS --fail-with-body "http://localhost:8000/backtest/factor-research/plot?research_group_id=strategy4_1h_20260531_20260804_v1&require_all_factors=true&pnl_tie_tolerance=0.01&minimum_pnl_margin=0.01"' \
	> factor_research_charts.zip

	rm -rf factor_research_charts
	mkdir factor_research_charts
	unzip -q factor_research_charts.zip \
		-d factor_research_charts	
	"""
	try:
		zip_buffer = (
			backtester_instance
			.build_factor_research_chart_zip(
				research_group_id=research_group_id,
				minimum_pnl_margin=minimum_pnl_margin,
				pnl_tie_tolerance=pnl_tie_tolerance,
				require_all_factors=require_all_factors,
			)
		)

		return StreamingResponse(
			zip_buffer,
			media_type="application/zip",
			headers={
				"Content-Disposition": (
					'attachment; '
					'filename="factor_research_charts.zip"'
				),
			},
		)

	except ValueError as exc:
		raise HTTPException(
			status_code=400,
			detail=str(
				exc
			),
		)

	except Exception:
		logger.exception(
			"Backtest factor-research plot failed"
		)

		raise HTTPException(
			status_code=500,
			detail=(
				"Backtest factor-research plot failed"
			),
		)


@app.delete("/backtest/factor-research")
def delete_backtest_factor_research(
	research_group_ids: Optional[str] = Query(
		default=None,
		description=(
			"Comma-separated research group IDs to delete"
		),
	),
	delete_all: bool = Query(
		default=False,
		description=(
			"Delete every factor-research record"
		),
	),
	confirm: bool = Query(
		default=False,
		description=(
			"Required when delete_all=true"
		),
	),
):
	"""
	Delete selected factor-research groups or all stored research data.
	E.g. Delete 1 group
	ssh -i ~/.ssh/my-aws-ec2-key ubuntu@54.176.151.9 \
	'curl -sS --fail-with-body -X DELETE "http://localhost:8000/backtest/factor-research?research_group_ids=strategy4_1h_20260531_20260804_v1"'	
	Delete several groups
	ssh -i ~/.ssh/my-aws-ec2-key ubuntu@54.176.151.9 \
	'curl -sS --fail-with-body -X DELETE "http://localhost:8000/backtest/factor-research?research_group_ids=strategy4_test_v1,strategy4_test_v2,strategy4_test_v3"'	
	Delete everything
	ssh -i ~/.ssh/my-aws-ec2-key ubuntu@54.176.151.9 \
	'curl -sS --fail-with-body -X DELETE "http://localhost:8000/backtest/factor-research?delete_all=true&confirm=true"'	
	"""
	try:
		if delete_all and not confirm:
			raise ValueError(
				"Deleting all factor-research data requires "
				"delete_all=true and confirm=true"
			)

		if delete_all and research_group_ids:
			raise ValueError(
				"Do not provide research_group_ids when "
				"delete_all=true"
			)

		group_id_list = None

		if research_group_ids:
			group_id_list = [
				group_id.strip()
				for group_id in (
					research_group_ids.split(
						","
					)
				)
				if group_id.strip()
			]

		return (
			backtester_instance
			.delete_factor_research_records(
				research_group_ids=group_id_list,
				delete_all=delete_all,
			)
		)

	except ValueError as exc:
		raise HTTPException(
			status_code=400,
			detail=str(
				exc
			),
		)

	except Exception:
		logger.exception(
			"Deleting backtest factor-research data failed"
		)

		raise HTTPException(
			status_code=500,
			detail=(
				"Deleting backtest factor-research data failed"
			),
		)		


@app.get("/backtest/factor-research/groups")
def list_backtest_factor_research_groups():
	"""
	List all stored factor-research groups and their metadata.
	ssh -i ~/.ssh/my-aws-ec2-key ubuntu@54.176.151.9 \
	'curl -sS --fail-with-body "http://localhost:8000/backtest/factor-research/groups"' \
	| python3 -m json.tool	
	Then you can copy an exact group ID from the response into the deletion request:
	ssh -i ~/.ssh/my-aws-ec2-key ubuntu@54.176.151.9 \
	'curl -sS --fail-with-body -X DELETE "http://localhost:8000/backtest/factor-research?research_group_ids=strategy4_1h_20260531_20260804_v1"' \
	| python3 -m json.tool	
	"""
	try:
		return (
			backtester_instance
			.list_factor_research_groups()
		)

	except Exception:
		logger.exception(
			"Listing backtest factor-research groups failed"
		)

		raise HTTPException(
			status_code=500,
			detail=(
				"Listing backtest factor-research groups failed"
			),
		)		