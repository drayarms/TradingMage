"""Alpha Vantage news-sentiment retrieval and persistent caching for TradingMage."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class AlphaVantageNewsClient:
	"""Read-only client for Alpha Vantage NEWS_SENTIMENT data."""

	BASE_URL = "https://www.alphavantage.co/query"
	CACHE_VERSION = 1

	def __init__(
		self,
		api_key: Optional[str] = None,
		timeout_seconds: int = 30,
		cache_dir: Optional[str] = None,
	):
		self.api_key = api_key
		self.timeout_seconds = int(timeout_seconds)
		self._cache: dict[tuple[str, str, str, int], list[dict[str, Any]]] = {}

		configured_cache_dir = (
			cache_dir
			or os.getenv("ALPHA_VANTAGE_NEWS_CACHE_DIR")
			or str(Path(__file__).resolve().parent / ".alpha_vantage_news_cache")
		)
		self.cache_dir = Path(configured_cache_dir).expanduser().resolve()
		self.cache_dir.mkdir(parents=True, exist_ok=True)

	@property
	def configured(self) -> bool:
		return bool(self.api_key)

	def get_news_sentiment(
		self,
		ticker: str,
		start_dt: datetime,
		end_dt: datetime,
		limit: int = 1000,
	) -> list[dict[str, Any]]:
		"""Return normalized ticker-specific news events in [start_dt, end_dt]."""
		symbol = str(ticker).upper().strip()
		start_utc = self._as_utc(start_dt)
		end_utc = self._as_utc(end_dt)
		request_limit = max(1, min(int(limit), 1000))
		cache_key = (
			symbol,
			start_utc.isoformat(),
			end_utc.isoformat(),
			request_limit,
		)

		if cache_key in self._cache:
			return list(self._cache[cache_key])

		disk_events = self._read_persistent_cache(
			symbol=symbol,
			start_utc=start_utc,
			end_utc=end_utc,
			limit=request_limit,
		)
		if disk_events is not None:
			self._cache[cache_key] = list(disk_events)
			return list(disk_events)

		if not self.configured:
			raise ValueError(
				"Alpha Vantage is not configured. Set ALPHA_VANTAGE_KEY in the application environment."
			)

		params = {
			"function": "NEWS_SENTIMENT",
			"tickers": symbol,
			"time_from": start_utc.strftime("%Y%m%dT%H%M"),
			"time_to": end_utc.strftime("%Y%m%dT%H%M"),
			"sort": "EARLIEST",
			"limit": request_limit,
			"apikey": self.api_key,
		}

		request = Request(
			f"{self.BASE_URL}?{urlencode(params)}",
			headers={"User-Agent": "TradingMage/1.0"},
		)

		with urlopen(request, timeout=self.timeout_seconds) as response:
			payload = json.loads(response.read().decode("utf-8"))

		if payload.get("Error Message"):
			raise ValueError(f"Alpha Vantage error: {payload['Error Message']}")
		if payload.get("Information") and not payload.get("feed"):
			raise ValueError(f"Alpha Vantage: {payload['Information']}")
		if payload.get("Note") and not payload.get("feed"):
			raise ValueError(f"Alpha Vantage: {payload['Note']}")

		events = []
		for article in payload.get("feed") or []:
			event = self._normalize_article(article, symbol)
			if event is None:
				continue
			published_dt = datetime.fromisoformat(event["published_at"])
			if start_utc <= published_dt <= end_utc:
				events.append(event)

		events.sort(key=lambda row: row["published_at"])
		self._write_persistent_cache(
			symbol=symbol,
			start_utc=start_utc,
			end_utc=end_utc,
			limit=request_limit,
			events=events,
		)
		self._cache[cache_key] = list(events)
		return list(events)

	def _persistent_cache_path(
		self,
		symbol: str,
		start_utc: datetime,
		end_utc: datetime,
		limit: int,
	) -> Path:
		identity = json.dumps(
			{
				"version": self.CACHE_VERSION,
				"function": "NEWS_SENTIMENT",
				"ticker": symbol,
				"start": start_utc.isoformat(),
				"end": end_utc.isoformat(),
				"sort": "EARLIEST",
				"limit": int(limit),
			},
			sort_keys=True,
			separators=(",", ":"),
		)
		digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
		return self.cache_dir / f"{symbol}_{digest}.json"

	def _read_persistent_cache(
		self,
		symbol: str,
		start_utc: datetime,
		end_utc: datetime,
		limit: int,
	) -> Optional[list[dict[str, Any]]]:
		cache_path = self._persistent_cache_path(
			symbol,
			start_utc,
			end_utc,
			limit,
		)
		if not cache_path.is_file():
			return None

		try:
			with cache_path.open("r", encoding="utf-8") as handle:
				payload = json.load(handle)
		except (OSError, ValueError, TypeError):
			return None

		if payload.get("cache_version") != self.CACHE_VERSION:
			return None
		if payload.get("ticker") != symbol:
			return None
		if payload.get("start") != start_utc.isoformat():
			return None
		if payload.get("end") != end_utc.isoformat():
			return None
		if int(payload.get("limit") or 0) != int(limit):
			return None

		events = payload.get("events")
		if not isinstance(events, list):
			return None

		return [event for event in events if isinstance(event, dict)]

	def _write_persistent_cache(
		self,
		symbol: str,
		start_utc: datetime,
		end_utc: datetime,
		limit: int,
		events: list[dict[str, Any]],
	) -> None:
		cache_path = self._persistent_cache_path(
			symbol,
			start_utc,
			end_utc,
			limit,
		)
		payload = {
			"cache_version": self.CACHE_VERSION,
			"function": "NEWS_SENTIMENT",
			"ticker": symbol,
			"start": start_utc.isoformat(),
			"end": end_utc.isoformat(),
			"sort": "EARLIEST",
			"limit": int(limit),
			"fetched_at": datetime.now(timezone.utc).isoformat(),
			"events": events,
		}

		temporary_path = None
		try:
			with tempfile.NamedTemporaryFile(
				mode="w",
				encoding="utf-8",
				dir=str(self.cache_dir),
				prefix=f".{symbol}_",
				suffix=".tmp",
				delete=False,
			) as handle:
				temporary_path = Path(handle.name)
				json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
				handle.flush()
				os.fsync(handle.fileno())

			os.replace(temporary_path, cache_path)
		except OSError:
			if temporary_path is not None:
				try:
					temporary_path.unlink(missing_ok=True)
				except OSError:
					pass

	def _normalize_article(
		self,
		article: dict[str, Any],
		ticker: str,
	) -> Optional[dict[str, Any]]:
		published_dt = self._parse_alpha_time(article.get("time_published"))
		if published_dt is None:
			return None

		ticker_row = None
		for row in article.get("ticker_sentiment") or []:
			if str(row.get("ticker") or "").upper().strip() == ticker:
				ticker_row = row
				break

		if ticker_row is None:
			return None

		return {
			"ticker": ticker,
			"published_at": published_dt.isoformat(),
			"title": str(article.get("title") or "").strip(),
			"source": str(article.get("source") or "").strip(),
			"url": str(article.get("url") or "").strip(),
			"summary": str(article.get("summary") or "").strip(),
			"overall_sentiment_score": self._safe_float(article.get("overall_sentiment_score")),
			"overall_sentiment_label": str(article.get("overall_sentiment_label") or "").strip(),
			"ticker_sentiment_score": self._safe_float(ticker_row.get("ticker_sentiment_score")),
			"ticker_sentiment_label": str(ticker_row.get("ticker_sentiment_label") or "").strip(),
			"relevance_score": self._safe_float(ticker_row.get("relevance_score")),
		}

	@staticmethod
	def _safe_float(value: Any) -> Optional[float]:
		try:
			return float(value)
		except (TypeError, ValueError):
			return None

	@staticmethod
	def _parse_alpha_time(value: Any) -> Optional[datetime]:
		text = str(value or "").strip()
		for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M"):
			try:
				return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
			except ValueError:
				continue
		return None

	@staticmethod
	def _as_utc(value: datetime) -> datetime:
		if value.tzinfo is None:
			return value.replace(tzinfo=timezone.utc)
		return value.astimezone(timezone.utc)


class AlphaVantageNewsHistory:
	"""Time-bounded simulation view over already-fetched news events."""

	def __init__(self, events: list[dict[str, Any]]):
		self.events = sorted(events, key=lambda row: row["published_at"])

	def get_events_through(
		self,
		current_dt: datetime,
		ticker: Optional[str] = None,
		lookback_minutes: Optional[int] = None,
	) -> list[dict[str, Any]]:
		current_utc = AlphaVantageNewsClient._as_utc(current_dt)
		minimum_dt = None
		if lookback_minutes is not None:
			from datetime import timedelta
			minimum_dt = current_utc - timedelta(minutes=int(lookback_minutes))

		result = []
		for event in self.events:
			event_dt = datetime.fromisoformat(event["published_at"])
			if event_dt > current_utc:
				break
			if minimum_dt is not None and event_dt < minimum_dt:
				continue
			if ticker and event.get("ticker") != str(ticker).upper().strip():
				continue
			result.append(event)
		return result
