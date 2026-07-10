import os
from concurrent.futures import ThreadPoolExecutor
import logging
from datetime import datetime, timezone, date, timedelta
import statistics
import csv


logger = logging.getLogger("tv-webhook")

DIAGNOSTIC_TICKERS = {
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN",
    "META", "GOOGL", "JPM", "XOM", "SPY",
}

#DIAGNOSTIC_LOG_PATH = os.getenv(
    #"TV_TRADE_DIAGNOSTIC_LOG",
    #"/app/logs/trade_diagnostics.csv",
#)

LIVE_DIAGNOSTIC_LOG_PATH = os.getenv(
	"TV_LIVE_DIAGNOSTIC_LOG",
	"/app/logs/live_trade_diagnostics.csv",
)

BACKTEST_DIAGNOSTIC_LOG_PATH = os.getenv(
	"TV_BACKTEST_DIAGNOSTIC_LOG",
	"/app/logs/backtest_trade_diagnostics.csv",
)

class TradeRecords:
	def __init__(self, trading_view_webhook_helpers):
		self.tvw_helpers = trading_view_webhook_helpers
		self.r = trading_view_webhook_helpers.require_redis()
		self.stream_maxlen = int(os.getenv("TV_MAXLEN", "500"))
		self.trade_event_maxlen = int(os.getenv("TV_TRADE_EVENT_MAXLEN", str(self.stream_maxlen)))
		self.pnl_stream_maxlen = int(os.getenv("TV_PNL_MAXLEN", str(self.stream_maxlen)))		

	def _normalize_strategy(self, strategy_name: str) -> str:
		return str(strategy_name or "").strip()

	def _normalize_ticker(self, ticker: str) -> str:
		return str(ticker or "").upper().strip()

	def _parse_float(self, value, default=0.0) -> float:
		try:
			if value in (None, ""):
				return float(default)
			return float(value)
		except (TypeError, ValueError):
			return float(default)

	def _parse_int(self, value, default=0) -> int:
		try:
			if value in (None, ""):
				return int(default)
			return int(value)
		except (TypeError, ValueError):
			return int(default)

	def _to_timestamp(self, iso_str: str) -> float:
		return datetime.fromisoformat(str(iso_str).replace("Z", "+00:00")).timestamp()

	def _iso_now(self) -> str:
		return datetime.now(timezone.utc).isoformat()

	def _to_eastern_iso(self, iso_str: str) -> str:
		dt = datetime.fromisoformat(str(iso_str).replace("Z", "+00:00"))
		if dt.tzinfo is None:
			dt = dt.replace(tzinfo=timezone.utc)
		return dt.astimezone(self.tvw_helpers.eastern_tz).isoformat()		

	def _compute_unrealized_pnl(self, side: str, avg_price_per_share: float, market_price: float, num_shares: float) -> float:
		if num_shares <= 0:
			return 0.0

		if side == "long":
			return (market_price - avg_price_per_share) * num_shares

		if side == "short":
			return (avg_price_per_share - market_price) * num_shares

		raise ValueError(f"Invalid side: {side}")


	def log_trade_diagnostic(
		self,
		*,
		source,              # "live" or "backtest"
		strategy_name,
		ticker,
		event_type,          # "entry", "exit", "partial_exit", "add", "reject", "cancel"
		#signal=None,
		timeframe=None,
		side=None,           # "long", "short", "buy", "sell", "cover"
		requested_qty=None,
		#price=None,
		#avg_price=None,
		market_price=None,
		order_id=None,
		#realized_pnl=None,
		#unrealized_pnl=None,
		#position_qty_before=None,
		#position_qty_after=None,
		#bar_close_time=None,
		decision_time=None,
	):
		ticker = str(ticker or "").upper().strip()

		if ticker not in DIAGNOSTIC_TICKERS:
			return

		if source == "live":
			log_path = LIVE_DIAGNOSTIC_LOG_PATH
		elif source == "backtest":
			log_path = BACKTEST_DIAGNOSTIC_LOG_PATH
		else:
			raise ValueError(f"Unknown diagnostic source: {source}")			

		os.makedirs(os.path.dirname(log_path), exist_ok=True)

		file_exists = os.path.exists(log_path)

		row = {
			"logged_at_utc": datetime.now(timezone.utc).isoformat(),
			"source": source,
			"strategy_name": strategy_name,
			"ticker": ticker,
			"event_type": event_type,
			#"signal": signal,
			"timeframe": timeframe,
			"side": side,
			#"qty": qty,
			"requested_qty": requested_qty,
			#"price": price,
			#"avg_price": avg_price,
			#"fill_price": fill_price,
			"market_price": market_price,
			"order_id": order_id,
			#"order_status": order_status,
			#"realized_pnl": realized_pnl,
			#"unrealized_pnl": unrealized_pnl,
			#"position_qty_before": position_qty_before,
			#"position_qty_after": position_qty_after,
			#"bar_close_time": bar_close_time,
			"decision_time": decision_time,
			#"reason": reason,
		}

		with open(log_path, "a", newline="") as f:
			writer = csv.DictWriter(f, fieldnames=row.keys())

			if not file_exists:
				writer.writeheader()

			writer.writerow(row)


	def _compute_realized_delta(self, side: str, avg_price_per_share: float, fill_price: float, close_qty: float) -> float:
		if close_qty <= 0:
			return 0.0

		if side == "long":
			return (fill_price - avg_price_per_share) * close_qty

		if side == "short":
			return (avg_price_per_share - fill_price) * close_qty

		raise ValueError(f"Invalid side: {side}")


	def get_current_trading_day(self) -> str:
		"""
		Returns the current trading day in Eastern Time as an ISO-formatted date string.

		The trading system uses U.S. market time (America/New_York) as the canonical
		reference for daily aggregations, exposure tracking, and risk reporting.
		This helper ensures that all daily calculations remain aligned to the market
		calendar regardless of the server's local timezone (e.g., UTC).

		Parameters:
			None

		Returns:
			str: The current trading day in YYYY-MM-DD format based on Eastern Time.

		Example:
			If the server time is:
				2026-06-02 00:30:00 UTC

			and Eastern Time is:
				2026-06-01 20:30:00 EDT

			then this function returns:
				"2026-06-01"
		"""
		return datetime.now(self.tvw_helpers.eastern_tz).date().isoformat()


	def get_market_prices(self, tickers, alpaca_api):
		"""
		Retrieves current market prices (bid, ask, and midpoint) for a list of tickers
		using the provided API client. Executes concurrent requests via threading,
		retries failed attempts up to a fixed limit, and aggregates results into a
		dictionary keyed by ticker.

		Parameters:
			tickers (list[str]): A list of ticker symbols (e.g., ["AAPL", "TSLA"]).
			alpaca_api (REST): Alpaca API client used to fetch latest quote data.

		Returns:
			dict: A dictionary mapping each ticker to its corresponding market data,
			including bid, ask, and calculated midpoint price ("market").
		"""		
		market_prices = {
			ticker: {
				"ticker": ticker,
				"ask": None,
				"bid": None,
				"market": None,
			}
			for ticker in tickers
		}

		def get_prices(ticker):
			trial = 0

			while trial < 3:
				try:
					last_quote = alpaca_api.get_latest_quote(ticker)
					ask = last_quote.ap
					bid = last_quote.bp

					if ask is None or bid is None:
						raise ValueError(f"Missing ask/bid for {ticker}: ask={ask}, bid={bid}")

					market = (ask + bid) / 2.0

					market_prices[ticker]["ask"] = ask
					market_prices[ticker]["bid"] = bid
					market_prices[ticker]["market"] = market
					return

				except Exception as e:
					trial += 1
					logger.info(
						"Unable to obtain market price for %s on attempt %d: %s",
						ticker,
						trial,
						e,
					)


		from concurrent.futures import ThreadPoolExecutor
		max_workers = min(5, len(tickers))
		with ThreadPoolExecutor(max_workers=max_workers) as executor:
			executor.map(get_prices, tickers)

		return market_prices


	def _entry_in_range(self, snapshot_time: str, start: str = None, end: str = None) -> bool:
		"""
		Determines whether a given snapshot timestamp falls within an optional
		start and end time range. Converts all timestamps to comparable numeric
		values before performing boundary checks.

		Parameters:
			snapshot_time (str): The ISO timestamp of the snapshot entry.
			start (str, optional): Inclusive start time boundary in ISO format.
			end (str, optional): Inclusive end time boundary in ISO format.

		Returns:
			bool: True if the snapshot_time falls within the specified range,
			or if no bounds exclude it; otherwise False.
		"""		
		entry_ts = self._to_timestamp(snapshot_time)

		if start:
			start_ts = self._to_timestamp(start)
			if entry_ts < start_ts:
				return False

		if end:
			end_ts = self._to_timestamp(end)
			if entry_ts > end_ts:
				return False

		return True


	def reset_tv_data(self):
		"""
		Deletes all Redis keys under the "tv:*" namespace in batches to efficiently
		clear application data. Iterates through matching keys, removes them in chunks,
		and logs the total number of keys deleted.

		Parameters:
			None

		Returns:
			dict: A summary containing the total number of deleted Redis keys.
		"""		
		deleted = 0
		batch = []

		for key in self.r.scan_iter("tv:*"):
			batch.append(key)

			if len(batch) >= 500:
				deleted += self.r.delete(*batch)
				batch = []

		if batch:
			deleted += self.r.delete(*batch)

		logger.warning("Deleted %d Redis keys under namespace tv:*", deleted)
		return {"deleted_keys": deleted}
