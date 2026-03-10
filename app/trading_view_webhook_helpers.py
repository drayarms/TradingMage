from datetime import datetime, timezone
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
		self.pacific_tz = ZoneInfo("America/Los_Angeles")

		try:
			self.r = redis.Redis.from_url(redis_url, decode_responses=True)
			self.r.ping()
		except Exception:
			logger.exception("Redis initialization failed")
			self.r = None
					

		if not tv_webhook_secret:
			raise RuntimeError("Missing TV_WEBHOOK_SECRET environment variable")

	def utc_now_iso(self) -> str:
		return datetime.now(timezone.utc).isoformat()

	def normalize_tf(self, timeframe: str) -> str:
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

	def normalize_signal(self, sig: str) -> str:
		s = (sig or "").strip().lower()
		if s.startswith("buy"):
			return "buy"
		if s.startswith("sell"):
			return "sell"
		return s

	def h(self, s: str) -> str:
		return hashlib.sha256((s or "").encode()).hexdigest()[:12]

	def stream_key(self, timeframe: str, symbol: str) -> str:
		tf = self.normalize_tf(timeframe)
		sym = str(symbol or "").upper().strip()
		return f"tv:stream:{tf}:{sym}"

	def state_key(self, timeframe: str, symbol: str) -> str:
		tf = self.normalize_tf(timeframe)
		sym = str(symbol or "").upper().strip()
		return f"tv:state:{tf}:{sym}"

	def to_str(self, value: Any) -> str:
		if value is None:
			return ""
		return str(value)

	def safe_float(self, value: Optional[str]) -> Optional[float]:
		if value in (None, ""):
			return None
		try:
			return float(value)
		except (TypeError, ValueError):
			logger.warning("Float conversion failed for value=%r", value)
			return None

	def parse_iso_to_pacific(self, iso_str: Optional[str]) -> Optional[str]:
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

	def require_redis(self):
		if self.r is None:
			raise HTTPException(status_code=503, detail="Redis not connected")
		return self.r


	def log_nth_last_alert(self, symbol, timeframe, n):
		r = self.require_redis()
		stream = self.stream_key(timeframe, symbol)
		entries = r.xrevrange(stream, count=n)

		if len(entries) < n:
			logger.info("No %sth entry for %r:%r", n, symbol, timeframe)
			return None

		entry = entries[n - 1]
		logger.info("Nth last alert for %r:%r => %r", symbol, timeframe, entry)
		return entry


	def get_nth_last_alert(self, symbol, timeframe, n):
		r = self.require_redis()
		stream = self.stream_key(timeframe, symbol)
		entries = r.xrevrange(stream, count=n)

		if len(entries) < n:
			return None

		return entries[n - 1]


	def handle_alert(self, ticker, tf, signal):
		last_alert = self.get_nth_last_alert(ticker, tf, 2)
		logger.info(
			"Handling Ticker=%r TF=%r Signal=%r SecondLastAlert=%r",
			ticker, tf, signal, last_alert
		)
		return last_alert


