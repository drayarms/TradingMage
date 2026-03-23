from datetime import datetime, timezone, time
from typing import Any, Optional
from zoneinfo import ZoneInfo

import hashlib
import redis
from fastapi import HTTPException

import logging

logger = logging.getLogger("tv-webhook")

class TradingViewWebhookHelpers:
	def __init__(self, tv_webhook_secret: str, redis_url: str):
		self.tv_webhook_secret = tv_webhook_secret
		self.redis_module = redis
		self.redis_url = redis_url
		self.eastern_tz = ZoneInfo("America/New_York")
		self.pacific_tz = ZoneInfo("America/Los_Angeles")
		self.alert_dedupe_ttl_seconds = 60 * 60 * 24 * 7  # 7 days

		try:
			self.r = redis.Redis.from_url(redis_url, decode_responses=True)
			self.r.ping()
		except Exception:
			logger.exception("Redis initialization failed")
			self.r = None
					
		if not tv_webhook_secret:
			raise RuntimeError("Missing TV_WEBHOOK_SECRET environment variable")


	def _now_et(self):
		return datetime.now(self.eastern_tz)


	def is_between_8pm_sun_and_8pm_fri_et(self, now_et=None):
		"""
		True only during the trading window:
		- starts Sunday 8:00 PM ET
		- ends Friday 8:00 PM ET
		"""
		now_et = now_et or self._now_et()
		weekday = now_et.weekday()  # Mon=0 ... Sun=6
		t = now_et.timetz()

		if weekday == 5:  # Saturday
			return False

		if weekday == 6:  # Sunday
			return t >= time(20, 0, tzinfo=self.eastern_tz)

		if weekday == 4:  # Friday
			return t < time(20, 0, tzinfo=self.eastern_tz)

		# Monday through Thursday
		return True


	def _is_regular_hours_et(self, now_et=None):
		"""
		Regular market hours only:
		9:30 AM ET <= time < 4:00 PM ET
		"""
		now_et = now_et or self._now_et()
		t = now_et.timetz()
		return time(9, 30, tzinfo=self.eastern_tz) <= t < time(16, 0, tzinfo=self.eastern_tz)


	def _get_asset_or_none(self, alpaca_api, symbol):
		try:
			return alpaca_api.get_asset(symbol)
		except Exception:
			logger.exception("Failed to fetch Alpaca asset for %r", symbol)
			return None


	def is_symbol_tradable_now(self, alpaca_api, symbol, now_et=None):
		"""
		Inside regular hours:
			asset must be active + tradable
		Outside regular hours:
			asset must be active + tradable + overnight_tradable
		"""
		now_et = now_et or self._now_et()
		asset = self._get_asset_or_none(alpaca_api, symbol)
		if asset is None:
			return False

		is_active = getattr(asset, "status", None) == "active"
		is_tradable = bool(getattr(asset, "tradable", False))

		if not (is_active and is_tradable):
			return False

		if self._is_regular_hours_et(now_et):
			return True

		return bool(getattr(asset, "overnight_tradable", False))


	def is_symbol_tradable(self, alpaca_api, symbol):
		"""
		Returns True if symbol in question is tradable, otherwise, False.
		Parameters:
			alpaca_api (REST):
			symblol (str): Ticker symbol.
		Returns:

		"""
		try:
			asset = alpaca_api.get_asset(symbol)
		except Exception:
			logger.exception("Failed to fetch Alpaca asset for %r", symbol)
			return False

		return bool(
			getattr(asset, "status", None) == "active" and
			getattr(asset, "tradable", False)
		)


	def is_symbol_shortable(self, alpaca_api, symbol):
		"""
		Returns True if symbol in question is shortable, otherwise, False.
		Parameters:
			alpaca_api (REST):
			symblol (str): Ticker symbol.
		Returns:

		"""
		try:
			asset = alpaca_api.get_asset(symbol)
		except Exception:
			logger.exception("Failed to fetch Alpaca asset for %r", symbol)
			return False

		return bool(
			getattr(asset, "status", None) == "active" and
			getattr(asset, "tradable", False) and
			getattr(asset, "shortable", False)
		)		


	def build_alert_idempotency_key(
		self,
		symbol: str,
		timeframe: str,
		signal: str,
		bar_close_time: str,
	) -> str:
		"""
		Builds a unique fingerpirnt for each alert so that can be compared against an already executed
		alert to ensure idempotency (same alert doesn't trigger an actioin more than once).
		Parameters:

		Returns:
		"""
		sym = str(symbol or "").upper().strip()
		tf = self.normalize_tf(timeframe)
		sig = str(signal or "").strip()
		bar_time = str(bar_close_time or "").strip()

		fingerprint = f"{sym}|{tf}|{sig}|{bar_time}"
		digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()

		return f"tv:idempotency:{digest}"

	def acquire_alert_idempotency(
		self,
		symbol: str,
		timeframe: str,
		signal: str,
		bar_close_time: str,
		ttl_seconds: Optional[int] = None,
	) -> tuple[bool, str]:
		"""
		Retrieves the idempotency fingerprint for a specific alert as specified by the parameters passed, 
		if an alert with that specific fingerprint doesn't exist yet otherwise, fails to retrieve it.
		Parameters:
			symbol (str):
			timeframe (str):
			signal (str):
			bar_close_time (str):
			ttl_seconds (int):
		Returns:
			bool, key (tuple):
		"""
		r = self.require_redis()

		key = self.build_alert_idempotency_key(
			symbol=symbol,
			timeframe=timeframe,
			signal=signal,
			bar_close_time=bar_close_time,
		)

		ttl = ttl_seconds or self.alert_dedupe_ttl_seconds

		acquired = r.set(
			name=key,
			value="processing",
			nx=True,
			ex=ttl,
		)

		return bool(acquired), key

	def utc_now_iso(self) -> str:
		"""
		Returns the current time in UTC (Coordinated Universal Time) e.g 2026-03-18T06:52:31.123456+00:00
		2026-03-18				date (YYYY-MM-DD)
		T						separator (ISO 8601 standard)
		06:52:31.123456			time (HH:MM:SS.microseconds)
		+00:00					UTC offset		
		"""
		return datetime.now(timezone.utc).isoformat()

	def normalize_tf(self, timeframe: str) -> str:
		"""
		Standardizes timeframe format.
		Parameters:
			timeframe (str): Timeframe
		Returns:
			(str): Standardized timeframe. 
		"""
		tf = str(timeframe or "").strip().lower()

		aliases = {
			"1": "1m",
			"3": "3m",
			"5": "5m",
			"15": "15m",
			"30": "30m",
			"45": "45m",
			"60": "1h",
			"120": "2h",
			"240": "4h",
			"d": "1d",
			"1d": "1d",
			"w": "1w",
			"1w": "1w",
		}

		return aliases.get(tf, tf)

	def utc_iso_to_pacific(self, iso_str: str) -> str:
		if not iso_str:
			return ""

		if iso_str.endswith("Z"):
			iso_str = iso_str.replace("Z", "+00:00")

		try:
			dt_utc = datetime.fromisoformat(iso_str)
			return dt_utc.astimezone(self.pacific_tz).isoformat()
		except Exception:
			logger.warning("Time conversion failed")
			return iso_str

	def utc_iso_to_eastern(self, iso_str: str) -> str:
		if not iso_str:
			return ""

		if iso_str.endswith("Z"):
			iso_str = iso_str.replace("Z", "+00:00")

		try:
			dt_utc = datetime.fromisoformat(iso_str)
			return dt_utc.astimezone(self.eastern_tz).isoformat()
		except Exception:
			logger.warning("Time conversion failed")
			return iso_str			

	def normalize_signal(self, sig: str) -> str:
		"""
		Normalized strings like "buy+" and "sell+" to just "buy" and "sell" respectively.
		Parameters:
			sig (str): Signal: Potential values -> "buy", "sell", "buy+", "sell+".
		Returns:
			s (str): Normalized string, "buy" or "sell".
		"""
		s = (sig or "").strip().lower()
		if s.startswith("buy"):
			return "buy"
		if s.startswith("sell"):
			return "sell"
		return s

	def h(self, s: str) -> str:
		return hashlib.sha256((s or "").encode()).hexdigest()[:12]

	def stream_key(self, timeframe: str, symbol: str) -> str:
		"""
		Returns a structured Redis key for storing streaming data for a specific timeframe and symbol.
		Used to log event histories by appending new data to the stream e.g TV alerts, price updates over time, trade exec logs.
		E.g.
		tv                    The namespace
		└── stream            
			└── 15m
				└── AAPL
		Parameters:
			timeframe (str)
			symbol (str)
		Returns:
			str	
		"""
		tf = self.normalize_tf(timeframe)
		sym = str(symbol or "").upper().strip()
		return f"tv:stream:{tf}:{sym}"

	def state_key(self, timeframe: str, symbol: str) -> str:
		"""
		Returns a structured Redis key for logging the current snapshot (latest state) for a specific timeframe and symbol.
		Overwrites prev vals. Only stores lates. Fast lookups. No history. Used for latesst signal per ticker, current position
		state, most recent ohlcv data.
		E.g.
		tv                    The namespace
		└── state           
			└── 15m
				└── AAPL
		Parameters:
			timeframe (str)
			symbol (str)
		Returns:
			str	 				
		"""
		tf = self.normalize_tf(timeframe)
		sym = str(symbol or "").upper().strip()
		return f"tv:state:{tf}:{sym}"

	def to_str(self, value: Any) -> str:
		"""
		Converts passed argument to string.
		Parameters:
			value (Any): Value to be converted to string.
		Returns:
			value (str): Value converted to a string or empty string if "None" was passed.
		"""
		if value is None:
			return ""
		return str(value)

	def safe_float(self, value: Optional[str]) -> Optional[float]:
		"""
		Converts passed argument to float.
		Parameters:
			value (Any): Value to be converted to float.
		Returns:
			value (float): Value converted to a float or None.		
		"""
		if value in (None, ""):
			return None
		try:
			return float(value)
		except (TypeError, ValueError):
			logger.warning("Float conversion failed for value=%r", value)
			return None

	def parse_iso_to_pacific(self, iso_str: Optional[str]) -> Optional[str]:
		"""
		Converts iso formated UTC date to PST
		Parameter:
			iso_str (str): iso formated UTC date
		"""
		if not iso_str:
			return None
		try:
			dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
			if dt.tzinfo is None:
				dt = dt.replace(tzinfo=timezone.utc)
			return dt.astimezone(self.pacific_tz).isoformat()
		except Exception:
			logger.warning("Datetime conversion failed for iso_str=%r", iso_str)
			return None	

	def parse_iso_to_eastern(self, iso_str: Optional[str]) -> Optional[str]:
		"""
		Converts iso formated UTC date to EST
		Parameter:
			iso_str (str): iso formated UTC date
		"""		
		if not iso_str:
			return None
		try:
			dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
			if dt.tzinfo is None:
				dt = dt.replace(tzinfo=timezone.utc)
			return dt.astimezone(self.eastern_tz).isoformat()
		except Exception:
			logger.warning("Datetime conversion failed for iso_str=%r", iso_str)
			return None		

	def require_redis(self):
		"""
		Returns the Redis client instance.
		"""
		if self.r is None:
			raise HTTPException(status_code=503, detail="Redis not connected")
		return self.r


	def log_nth_last_alert(self, symbol, timeframe, n):
		r = self.require_redis()
		stream = self.stream_key(timeframe, symbol) # E.g. "tv:stream:15m:AAPL"
		entries = r.xrevrange(stream, count=n) # Reads entries from a Redis stream in reverse order (newest -> oldest)

		if len(entries) < n:
			logger.info("No %sth entry for %r:%r", n, symbol, timeframe)
			return None

		entry = entries[n - 1]
		logger.info("Nth last alert for %r:%r => %r", symbol, timeframe, entry)
		return entry


	def get_nth_last_alert(self, symbol, timeframe, n):
		"""
		Retrieves the last nth alert received from TV for the ticker/timeframe pair specified.
		Parameters:
			symbol (str): The ticker symbol
			timeframe (str): The timeframe
			n (int): The last nth position
		Returns:
			entries[n - 1] (tuple): The alert ID and a dictionary containing imformation about the price 
			and ohlcv data for the symbol/timeframe pair.
		"""
		r = self.require_redis()
		stream = self.stream_key(timeframe, symbol) # E.g. "tv:stream:15m:AAPL"
		entries = r.xrevrange(stream, count=n) # Reads entries from a Redis stream in reverse order (newest -> oldest)

		if len(entries) < n:
			return None

		return entries[n - 1]


	def handle_alert(self, ticker, tf, signal):
		"""
		This function just tests the correctness of the get_nth_last_alert() function by comparing the output to that
		actually sent by TV.
		Parameters:
			ticker (str): The ticker symbol.
			tf (str): The timeframe.
			signal (str): The signal, "buy", "sell", "buy+", or "sell+".
		Returns:
			last_alert (tuple): The alert ID and a dictionary containing imformation about the price 
			and ohlcv data for the symbol/timeframe pair.
		"""
		last_alert = self.get_nth_last_alert(ticker, tf, 2)
		logger.info(
			"Handling Ticker=%r TF=%r Signal=%r SecondLastAlert=%r",
			ticker, tf, signal, last_alert
		)
		return last_alert


