import os
import threading
import logging
from datetime import datetime, timezone

logger = logging.getLogger("tv-webhook")

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

	def _position_key(self, strategy_name: str, ticker: str) -> str:
		strat = self._normalize_strategy(strategy_name)
		sym = self._normalize_ticker(ticker)
		return f"tv:position:{strat}:{sym}"

	def _strategy_positions_key(self, strategy_name: str) -> str:
		strat = self._normalize_strategy(strategy_name)
		return f"tv:positions:{strat}"

	def _positions_index_key(self) -> str:
		return "tv:positions:index"

	def _trade_event_stream_key(self) -> str:
		return "tv:trade_events"

	def _trade_index_key(self) -> str:
		return "tv:trades:index"		

	def _trade_event_pair_stream_key(self, strategy_name: str, ticker: str) -> str:
		strat = self._normalize_strategy(strategy_name)
		sym = self._normalize_ticker(ticker)
		return f"tv:trade_events:{strat}:{sym}"

	def _pnl_stream_key(self, strategy_name: str, ticker: str) -> str:
		strat = self._normalize_strategy(strategy_name)
		sym = self._normalize_ticker(ticker)
		return f"tv:pnl:{strat}:{sym}"

	def _aggregate_pnl_stream_key(self, strategy_name: str) -> str:
		strat = self._normalize_strategy(strategy_name)
		return f"tv:pnl:{strat}:ALL"

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


	def _compute_realized_delta(self, side: str, avg_price_per_share: float, fill_price: float, close_qty: float) -> float:
		if close_qty <= 0:
			return 0.0

		if side == "long":
			return (fill_price - avg_price_per_share) * close_qty

		if side == "short":
			return (avg_price_per_share - fill_price) * close_qty

		raise ValueError(f"Invalid side: {side}")


	def _load_position(self, strategy_name: str, ticker: str):
		"""
		Retrieves the current position for a given strategy and ticker from Redis.
		If the position does not exist or contains no data, returns None. Otherwise,
		returns a normalized dictionary with parsed numeric fields for downstream use.

		Parameters:
			strategy_name (str): The name of the trading strategy.
			ticker (str): The ticker symbol (e.g., "AAPL").

		Returns:
			Optional[dict]: A dictionary containing the position data, including pricing,
			share count, and PnL metrics, or None if no position exists.
		"""		
		key = self._position_key(strategy_name, ticker)
		if not self.r.exists(key):
			return None

		fields = self.r.hgetall(key)
		if not fields:
			return None

		return {
			"key": key,
			"strategy_name": fields.get("strategy_name"),
			"ticker": fields.get("ticker"),
			"side": fields.get("side"),
			"entry_date": fields.get("entry_date"),
			"entry_price": self._parse_float(fields.get("entry_price")),
			"entry_price_per_share": self._parse_float(fields.get("entry_price_per_share")),
			"avg_price_per_share": self._parse_float(fields.get("avg_price_per_share")),
			"last_market_price": self._parse_float(fields.get("last_market_price")),
			"num_shares": self._parse_float(fields.get("num_shares")),
			"unrealized_pnl": self._parse_float(fields.get("unrealized_pnl")),
			"realized_pnl": self._parse_float(fields.get("realized_pnl")),
		}


	def get_position(self, strategy_name: str, ticker: str):
		"""
		Retrieves the current position for a given strategy and ticker by delegating
		to the internal _load_position method.

		Parameters:
			strategy_name (str): The name of the trading strategy.
			ticker (str): The ticker symbol (e.g., "AAPL").

		Returns:
			Optional[dict]: The position data dictionary if it exists, or None if no position is found.
		"""
		return self._load_position(strategy_name, ticker)


	def record_trade_event(
		self,
		strategy_name: str,
		ticker: str,
		event_type: str,
		event_time: str,
		price: float,
		num_shares: float,
		side: str,
		old_num_shares: float,
		new_num_shares: float,
		realized_delta: float,
		cumulative_realized_pnl: float,
	):
		"""
		Records a trade-related event (e.g., open, close, modify) to Redis streams
		for both global and strategy/ticker-specific tracking. Normalizes inputs,
		converts timestamps to Eastern Time, and stores all numeric values as strings
		for consistency with Redis.

		Parameters:
			strategy_name (str): The name of the trading strategy.
			ticker (str): The ticker symbol (e.g., "AAPL").
			event_type (str): The type of trade event (e.g., "open", "close", "modify").
			event_time (str): ISO timestamp of the event; defaults to current time if not provided.
			price (float): The execution or reference price for the trade.
			num_shares (float): Number of shares involved in the event.
			side (str): Trade direction ("long" or "short").
			old_num_shares (float): Previous share count before the event.
			new_num_shares (float): Updated share count after the event.
			realized_delta (float): Realized PnL change from this event.
			cumulative_realized_pnl (float): Total realized PnL after this event.

		Returns:
			dict: Metadata about the recorded event, including stream keys, stream IDs,
			and the stored fields dictionary.
		"""	
		strat = self._normalize_strategy(strategy_name)
		sym = self._normalize_ticker(ticker)
		event_time = self._to_eastern_iso(event_time or self._iso_now())

		fields = {
			"strategy_name": strat,
			"ticker": sym,
			"event_type": str(event_type or "").strip().lower(),
			"event_time": event_time,
			"price": self.tvw_helpers.to_str(price),
			"num_shares": self.tvw_helpers.to_str(num_shares),
			"side": str(side or "").strip().lower(),
			"old_num_shares": self.tvw_helpers.to_str(old_num_shares),
			"new_num_shares": self.tvw_helpers.to_str(new_num_shares),
			"realized_delta": self.tvw_helpers.to_str(realized_delta),
			"cumulative_realized_pnl": self.tvw_helpers.to_str(cumulative_realized_pnl),
		}

		global_stream = self._trade_event_stream_key()
		pair_stream = self._trade_event_pair_stream_key(strat, sym)

		pipe = self.r.pipeline()
		pipe.xadd(
			global_stream,
			fields,
			maxlen=self.trade_event_maxlen,
			approximate=True,
		)
		pipe.xadd(
			pair_stream,
			fields,
			maxlen=self.trade_event_maxlen,
			approximate=True,
		)
		results = pipe.execute()

		return {
			"global_stream": global_stream,
			"global_stream_id": results[0],
			"pair_stream": pair_stream,
			"pair_stream_id": results[1],
			"fields": fields,
		}

	def _strategies_index_key(self) -> str:
		"""
		Returns the Redis key used to store the set of all strategy names.
		This index enables quick lookup and iteration over all strategies
		that currently have recorded positions or activity.

		Parameters:
			None

		Returns:
			str: The Redis key for the global strategies index.
		"""		
		return "tv:strategies:index"


	def create_trade_record(self, strategy_name, ticker, date, price, num_shares, side, can_add_to_existing_position):
		"""
		Create or update a trade record for a strategy/ticker.

		Supported sides:
			- "long": open a long position, or add to an existing long position
			- "short": open a short position, or add to an existing short position
			- "sell": close/reduce an existing long position
			- "cover": close/reduce an existing short position

		If can_add_to_existing_position is True and a same-side open position already exists,
		then for "long" / "short" this function adds the provided num_shares to the existing
		position instead of raising.

		Returns:
			dict with updated position and created event metadata.
		"""
		strat = self._normalize_strategy(strategy_name)
		sym = self._normalize_ticker(ticker)
		side = str(side or "").strip().lower()

		if side not in {"long", "short", "sell", "cover"}:
			raise ValueError(f"Invalid trade side: {side}")

		num_shares = self._parse_float(num_shares)
		if num_shares <= 0:
			raise ValueError("num_shares must be > 0")

		price = self._parse_float(price)
		if price <= 0:
			raise ValueError("price must be > 0")

		iso_dt = self._to_eastern_iso(date or self._iso_now())

		existing = self._load_position(strat, sym)

		position_key = self._position_key(strat, sym)
		strategy_positions_key = self._strategy_positions_key(strat)
		global_index_key = self._positions_index_key()
		strategies_index_key = self._strategies_index_key()
		trade_index_key = self._trade_index_key()

		# --------------------------------------------------
		# OPEN / REOPEN / ADD
		# --------------------------------------------------
		if side in {"long", "short"}:
			if existing is not None and existing["num_shares"] > 0:
				existing_side = str(existing["side"]).strip().lower()

				if existing_side != side:
					raise ValueError(
						f"Open position already exists for strategy/ticker with opposite side. existing_side={existing_side} requested_side={side}"
					)

				if not can_add_to_existing_position:
					raise ValueError("Open position already exists for strategy/ticker. Close or modify it first.")

				old_qty = self._parse_float(existing["num_shares"])
				old_avg = self._parse_float(existing["avg_price_per_share"])
				old_realized = self._parse_float(existing["realized_pnl"])
				old_entry_date = existing["entry_date"]
				old_entry_price = self._parse_float(existing["entry_price"])
				old_entry_price_per_share = self._parse_float(existing["entry_price_per_share"])

				new_qty = old_qty + num_shares
				new_avg = ((old_avg * old_qty) + (price * num_shares)) / new_qty
				new_last_market_price = price
				new_unrealized = self._compute_unrealized_pnl(side, new_avg, new_last_market_price, new_qty)

				updated_record = {
					"strategy_name": strat,
					"ticker": sym,
					"side": side,
					"entry_date": old_entry_date,
					"entry_price": self.tvw_helpers.to_str(old_entry_price),
					"entry_price_per_share": self.tvw_helpers.to_str(old_entry_price_per_share),
					"avg_price_per_share": self.tvw_helpers.to_str(new_avg),
					"last_market_price": self.tvw_helpers.to_str(new_last_market_price),
					"num_shares": self.tvw_helpers.to_str(new_qty),
					"unrealized_pnl": self.tvw_helpers.to_str(new_unrealized),
					"realized_pnl": self.tvw_helpers.to_str(old_realized),
				}

				self.r.hset(position_key, mapping=updated_record)

				event = self.record_trade_event(
					strategy_name=strat,
					ticker=sym,
					event_type="add",
					event_time=iso_dt,
					price=price,
					num_shares=num_shares,
					side=side,
					old_num_shares=old_qty,
					new_num_shares=new_qty,
					realized_delta=0.0,
					cumulative_realized_pnl=old_realized,
				)

				ts = datetime.fromisoformat(iso_dt).timestamp()
				self.r.zadd(trade_index_key, {
					f"{strat}|{sym}|{event['global_stream_id']}": ts
				})

				logger.info(
					"Added to existing position for strategy=%r ticker=%r side=%r add_qty=%r old_qty=%r new_qty=%r",
					strat,
					sym,
					side,
					num_shares,
					old_qty,
					new_qty,
				)

				return {
					"position": self.get_position(strat, sym),
					"event": event,
				}

			carry_realized_pnl = 0.0
			event_type = "open"

			if existing is not None:
				carry_realized_pnl = self._parse_float(existing["realized_pnl"])
				if self._parse_float(existing["num_shares"]) == 0:
					event_type = "reopen"

			record = {
				"strategy_name": strat,
				"ticker": sym,
				"side": side,
				"entry_date": iso_dt,
				"entry_price": self.tvw_helpers.to_str(price),
				"entry_price_per_share": self.tvw_helpers.to_str(price),
				"avg_price_per_share": self.tvw_helpers.to_str(price),
				"last_market_price": self.tvw_helpers.to_str(price),
				"num_shares": self.tvw_helpers.to_str(num_shares),
				"unrealized_pnl": self.tvw_helpers.to_str(0.0),
				"realized_pnl": self.tvw_helpers.to_str(carry_realized_pnl),
			}

			pipe = self.r.pipeline()
			pipe.hset(position_key, mapping=record)
			pipe.sadd(strategy_positions_key, sym)
			pipe.sadd(global_index_key, f"{strat}|{sym}")
			pipe.sadd(strategies_index_key, strat)
			pipe.execute()

			event = self.record_trade_event(
				strategy_name=strat,
				ticker=sym,
				event_type=event_type,
				event_time=iso_dt,
				price=price,
				num_shares=num_shares,
				side=side,
				old_num_shares=0,
				new_num_shares=num_shares,
				realized_delta=0.0,
				cumulative_realized_pnl=carry_realized_pnl,
			)

			ts = datetime.fromisoformat(iso_dt).timestamp()
			self.r.zadd(trade_index_key, {
				f"{strat}|{sym}|{event['global_stream_id']}": ts
			})

			logger.info(
				"Created trade record for strategy=%r ticker=%r side=%r qty=%r",
				strat,
				sym,
				side,
				num_shares
			)

			return {
				"position": self.get_position(strat, sym),
				"event": event,
			}

		# --------------------------------------------------
		# CLOSE / REDUCE
		# --------------------------------------------------
		if existing is None or self._parse_float(existing["num_shares"]) <= 0:
			raise ValueError("No open position exists for strategy/ticker.")

		existing_side = str(existing["side"]).strip().lower()
		existing_num_shares = self._parse_float(existing["num_shares"])
		avg_price = self._parse_float(existing["avg_price_per_share"])
		current_realized_pnl = self._parse_float(existing["realized_pnl"])

		if side == "sell" and existing_side != "long":
			raise ValueError("Cannot sell: open position is not long.")

		if side == "cover" and existing_side != "short":
			raise ValueError("Cannot cover: open position is not short.")

		if num_shares > existing_num_shares:
			raise ValueError("Cannot close more shares than currently open.")

		if side == "sell":
			realized_delta = (price - avg_price) * num_shares
		else:
			realized_delta = (avg_price - price) * num_shares

		new_num_shares = existing_num_shares - num_shares
		new_cumulative_realized_pnl = current_realized_pnl + realized_delta

		updated_record = {
			"strategy_name": strat,
			"ticker": sym,
			"side": existing_side,
			"entry_date": existing["entry_date"],
			"entry_price": self.tvw_helpers.to_str(existing["entry_price"]),
			"entry_price_per_share": self.tvw_helpers.to_str(existing["entry_price_per_share"]),
			"avg_price_per_share": self.tvw_helpers.to_str(avg_price),
			"last_market_price": self.tvw_helpers.to_str(price),
			"num_shares": self.tvw_helpers.to_str(new_num_shares),
			"unrealized_pnl": self.tvw_helpers.to_str(0.0),
			"realized_pnl": self.tvw_helpers.to_str(new_cumulative_realized_pnl),
		}

		event_type = "partial_close"
		if new_num_shares == 0:
			event_type = "close"

		self.r.hset(position_key, mapping=updated_record)

		event = self.record_trade_event(
			strategy_name=strat,
			ticker=sym,
			event_type=event_type,
			event_time=iso_dt,
			price=price,
			num_shares=num_shares,
			side=side,
			old_num_shares=existing_num_shares,
			new_num_shares=new_num_shares,
			realized_delta=realized_delta,
			cumulative_realized_pnl=new_cumulative_realized_pnl,
		)

		ts = datetime.fromisoformat(iso_dt).timestamp()
		self.r.zadd(trade_index_key, {
			f"{strat}|{sym}|{event['global_stream_id']}": ts
		})

		logger.info(
			"Updated trade record for strategy=%r ticker=%r side=%r qty=%r old_qty=%r new_qty=%r realized_delta=%r",
			strat,
			sym,
			side,
			num_shares,
			existing_num_shares,
			new_num_shares,
			realized_delta,
		)

		return {
			"position": self.get_position(strat, sym),
			"event": event,
		}


	def create_trade_record_old(self, strategy_name, ticker, date, price, num_shares, side):
		"""
		Create or update a trade record for a strategy/ticker.

		Supported sides:
			- "long": open a long position
			- "short": open a short position
			- "sell": close/reduce an existing long position
			- "cover": close/reduce an existing short position

		Returns:
			dict with updated position and created event metadata.
		"""
		strat = self._normalize_strategy(strategy_name)
		sym = self._normalize_ticker(ticker)
		side = str(side or "").strip().lower()

		if side not in {"long", "short", "sell", "cover"}:
			raise ValueError(f"Invalid trade side: {side}")

		num_shares = self._parse_float(num_shares)
		if num_shares <= 0:
			raise ValueError("num_shares must be > 0")

		price = self._parse_float(price)
		if price <= 0:
			raise ValueError("price must be > 0")

		iso_dt = self._to_eastern_iso(date or self._iso_now())

		existing = self._load_position(strat, sym)

		position_key = self._position_key(strat, sym)
		strategy_positions_key = self._strategy_positions_key(strat)
		global_index_key = self._positions_index_key()
		strategies_index_key = self._strategies_index_key()
		trade_index_key = self._trade_index_key()

		# --------------------------------------------------
		# OPEN / REOPEN
		# --------------------------------------------------
		if side in {"long", "short"}:
			if existing is not None and existing["num_shares"] > 0:
				raise ValueError("Open position already exists for strategy/ticker. Close or modify it first.")

			carry_realized_pnl = 0.0
			event_type = "open"

			if existing is not None:
				carry_realized_pnl = existing["realized_pnl"]
				if existing["num_shares"] == 0:
					event_type = "reopen"

			record = {
				"strategy_name": strat,
				"ticker": sym,
				"side": side,
				"entry_date": iso_dt,
				"entry_price": self.tvw_helpers.to_str(price),
				"entry_price_per_share": self.tvw_helpers.to_str(price),
				"avg_price_per_share": self.tvw_helpers.to_str(price),
				"last_market_price": self.tvw_helpers.to_str(price),
				"num_shares": self.tvw_helpers.to_str(num_shares),
				"unrealized_pnl": self.tvw_helpers.to_str(0.0),
				"realized_pnl": self.tvw_helpers.to_str(carry_realized_pnl),
			}

			pipe = self.r.pipeline()
			pipe.hset(position_key, mapping=record)
			pipe.sadd(strategy_positions_key, sym)
			pipe.sadd(global_index_key, f"{strat}|{sym}")
			pipe.sadd(strategies_index_key, strat)
			pipe.execute()

			event = self.record_trade_event(
				strategy_name=strat,
				ticker=sym,
				event_type=event_type,
				event_time=iso_dt,
				price=price,
				num_shares=num_shares,
				side=side,
				old_num_shares=0,
				new_num_shares=num_shares,
				realized_delta=0.0,
				cumulative_realized_pnl=carry_realized_pnl,
			)

			ts = datetime.fromisoformat(iso_dt).timestamp()
			self.r.zadd(trade_index_key, {
				f"{strat}|{sym}|{event['global_stream_id']}": ts
			})

			logger.info(
				"Created trade record for strategy=%r ticker=%r side=%r qty=%r",
				strat,
				sym,
				side,
				num_shares
			)

			return {
				"position": self.get_position(strat, sym),
				"event": event,
			}

		# --------------------------------------------------
		# CLOSE / REDUCE
		# --------------------------------------------------
		if existing is None or existing["num_shares"] <= 0:
			raise ValueError("No open position exists for strategy/ticker.")

		existing_side = str(existing["side"]).strip().lower()
		existing_num_shares = self._parse_float(existing["num_shares"])
		entry_price = self._parse_float(existing["entry_price_per_share"])
		current_realized_pnl = self._parse_float(existing["realized_pnl"])

		if side == "sell" and existing_side != "long":
			raise ValueError("Cannot sell: open position is not long.")

		if side == "cover" and existing_side != "short":
			raise ValueError("Cannot cover: open position is not short.")

		if num_shares > existing_num_shares:
			raise ValueError("Cannot close more shares than currently open.")

		if side == "sell":
			realized_delta = (price - entry_price) * num_shares
		else:  # cover
			realized_delta = (entry_price - price) * num_shares

		new_num_shares = existing_num_shares - num_shares
		new_cumulative_realized_pnl = current_realized_pnl + realized_delta

		# Keep original entry details for any remaining open shares
		updated_record = {
			"strategy_name": strat,
			"ticker": sym,
			"side": existing_side,
			"entry_date": existing["entry_date"],
			"entry_price": self.tvw_helpers.to_str(entry_price),
			"entry_price_per_share": self.tvw_helpers.to_str(entry_price),
			"avg_price_per_share": self.tvw_helpers.to_str(entry_price),
			"last_market_price": self.tvw_helpers.to_str(price),
			"num_shares": self.tvw_helpers.to_str(new_num_shares),
			"unrealized_pnl": self.tvw_helpers.to_str(0.0),
			"realized_pnl": self.tvw_helpers.to_str(new_cumulative_realized_pnl),
		}

		# If fully closed, keep position record but zero out shares
		event_type = "partial_close"
		if new_num_shares == 0:
			event_type = "close"

		self.r.hset(position_key, mapping=updated_record)

		event = self.record_trade_event(
			strategy_name=strat,
			ticker=sym,
			event_type=event_type,
			event_time=iso_dt,
			price=price,
			num_shares=num_shares,
			side=side,
			old_num_shares=existing_num_shares,
			new_num_shares=new_num_shares,
			realized_delta=realized_delta,
			cumulative_realized_pnl=new_cumulative_realized_pnl,
		)

		ts = datetime.fromisoformat(iso_dt).timestamp()
		self.r.zadd(trade_index_key, {
			f"{strat}|{sym}|{event['global_stream_id']}": ts
		})

		logger.info(
			"Updated trade record for strategy=%r ticker=%r side=%r qty=%r old_qty=%r new_qty=%r realized_delta=%r",
			strat,
			sym,
			side,
			num_shares,
			existing_num_shares,
			new_num_shares,
			realized_delta,
		)

		return {
			"position": self.get_position(strat, sym),
			"event": event,
		}


	def snapshot_all_pnl(self, alpaca_api):
		"""
		Executes PnL snapshot calculations for all strategies currently tracked in Redis.
		Iterates through the global strategies index, invokes snapshot_pnl for each,
		and aggregates the results while handling failures per strategy.

		Parameters:
			alpaca_api (REST): Alpaca API client used to fetch market data for PnL calculations.

		Returns:
			dict: A summary containing the total number of strategies processed and a list
			of per-strategy results, including success status and either the snapshot result
			or an error message.
		"""		
		strategies = sorted(self.r.smembers(self._strategies_index_key()))
		results = []

		for strategy_name in strategies:
			try:
				result = self.snapshot_pnl(strategy_name, alpaca_api)
				results.append({
					"strategy_name": strategy_name,
					"ok": True,
					"result": result,
				})
			except Exception as exc:
				logger.exception("PnL snapshot failed for strategy=%r", strategy_name)
				results.append({
					"strategy_name": strategy_name,
					"ok": False,
					"error": str(exc),
				})

		return {
			"count": len(results),
			"results": results,
		}		

	def modify_trade_record(self, strategy_name, ticker, date, price, num_shares, action):
		"""
		Modifies an existing trade record by either increasing or reducing the position size.
		Validates inputs, recalculates average price, unrealized PnL, realized PnL, and share count,
		updates the stored position in Redis, and records a corresponding trade event reflecting
		the change.

		Parameters:
			strategy_name (str): The name of the trading strategy.
			ticker (str): The ticker symbol (e.g., "AAPL").
			date (str): ISO timestamp for the modification event; defaults to current time if not provided.
			price (float): Execution price for the added or reduced shares.
			num_shares (flaot): Number of shares to add or reduce (must be > 0).
			action (str): Modification type, either "add" or "reduce".

		Returns:
			dict: A dictionary containing the updated position state and the associated
			trade event metadata.
		"""		
		strat = self._normalize_strategy(strategy_name)
		sym = self._normalize_ticker(ticker)
		action = str(action or "").strip().lower()

		if action not in {"add", "reduce"}:
			raise ValueError(f"Invalid action: {action}")

		num_shares = self._parse_float(num_shares)
		if num_shares <= 0:
			raise ValueError("num_shares must be > 0")

		price = self._parse_float(price)
		if price <= 0:
			raise ValueError("price must be > 0")

		iso_dt = self._to_eastern_iso(date or self._iso_now())

		existing = self._load_position(strat, sym)
		if existing is None:
			raise ValueError("No record exists for strategy/ticker. Use create_trade_record first.")

		old_qty = existing["num_shares"]
		side = existing["side"]
		old_avg = existing["avg_price_per_share"]
		old_realized = existing["realized_pnl"]
		old_entry_date = existing["entry_date"]
		old_entry_price = existing["entry_price"]
		old_entry_price_per_share = existing["entry_price_per_share"]

		if side not in {"long", "short"}:
			raise ValueError(f"Invalid stored side: {side}")

		if old_qty == 0:
			if action == "reduce":
				raise ValueError("Cannot reduce a closed position.")
			return self.create_trade_record(
				strategy_name=strat,
				ticker=sym,
				date=iso_dt,
				price=price,
				num_shares=num_shares,
				side=side,
			)

		if action == "add":
			new_qty = old_qty + num_shares
			new_avg = ((old_avg * old_qty) + (price * num_shares)) / new_qty
			new_last_market_price = price
			new_unrealized = self._compute_unrealized_pnl(side, new_avg, new_last_market_price, new_qty)
			new_realized = old_realized
			realized_delta = 0.0
			event_type = "add"

		else:
			if num_shares > old_qty:
				raise ValueError("Cannot reduce more shares than currently open.")

			realized_delta = self._compute_realized_delta(side, old_avg, price, num_shares)
			new_realized = old_realized + realized_delta
			new_qty = old_qty - num_shares
			new_avg = old_avg
			new_last_market_price = price

			if new_qty == 0:
				new_unrealized = 0.0
				event_type = "close"
			else:
				new_unrealized = self._compute_unrealized_pnl(side, new_avg, new_last_market_price, new_qty)
				event_type = "reduce"

		updated = {
			"strategy_name": strat,
			"ticker": sym,
			"side": side,
			"entry_date": old_entry_date,
			"entry_price": self.tvw_helpers.to_str(old_entry_price),
			"entry_price_per_share": self.tvw_helpers.to_str(old_entry_price_per_share),
			"avg_price_per_share": self.tvw_helpers.to_str(new_avg),
			"last_market_price": self.tvw_helpers.to_str(new_last_market_price),
			"num_shares": self.tvw_helpers.to_str(new_qty),
			"unrealized_pnl": self.tvw_helpers.to_str(new_unrealized),
			"realized_pnl": self.tvw_helpers.to_str(new_realized),
		}

		position_key = self._position_key(strat, sym)
		self.r.hset(position_key, mapping=updated)

		event = self.record_trade_event(
			strategy_name=strat,
			ticker=sym,
			event_type=event_type,
			event_time=iso_dt,
			price=price,
			num_shares=num_shares,
			side=side,
			old_num_shares=old_qty,
			new_num_shares=new_qty,
			realized_delta=realized_delta,
			cumulative_realized_pnl=new_realized,
		)

		logger.info(
			"Modified trade record for strategy=%r ticker=%r action=%r old_qty=%r new_qty=%r",
			strat,
			sym,
			action,
			old_qty,
			new_qty,
		)

		return {
			"position": self.get_position(strat, sym),
			"event": event,
		}

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

		threads = []
		for ticker in tickers:
			t = threading.Thread(target=get_prices, args=(ticker,))
			threads.append(t)

		for t in threads:
			t.start()

		for t in threads:
			t.join()

		return market_prices

	def snapshot_pnl(self, strategy_name: str, alpaca_api):
		"""
		Calculates and records a point-in-time PnL snapshot for all positions associated
		with a given strategy. Fetches current market prices for open positions, updates
		stored position values in Redis, writes per-ticker and aggregate PnL snapshots
		to Redis streams, and returns a structured summary of the results.

		Parameters:
			strategy_name (str): The name of the trading strategy whose positions should be evaluated.
			alpaca_api (REST): Alpaca API client used to retrieve current market quote data.

		Returns:
			dict: A summary containing the snapshot timestamp, counts of tracked tickers and
			open positions, aggregate realized/unrealized/total PnL, and a list of per-ticker
			PnL records.
		"""		
		strat = self._normalize_strategy(strategy_name)
		tickers = sorted(self.r.smembers(self._strategy_positions_key(strat)))
		snapshot_time = self._to_eastern_iso(self._iso_now())

		if not tickers:
			aggregate_fields = {
				"snapshot_time": snapshot_time,
				"strategy_name": strat,
				"total_pnl": self.tvw_helpers.to_str(0.0),
				"total_unrealized_pnl": self.tvw_helpers.to_str(0.0),
				"total_realized_pnl": self.tvw_helpers.to_str(0.0),
				"tracked_tickers": self.tvw_helpers.to_str(0),
				"open_positions": self.tvw_helpers.to_str(0),
			}
			self.r.xadd(
				self._aggregate_pnl_stream_key(strat),
				aggregate_fields,
				maxlen=self.pnl_stream_maxlen,
				approximate=True,
			)
			return {
				"strategy_name": strat,
				"snapshot_time": snapshot_time,
				"tracked_tickers": 0,
				"open_positions": 0,
				"total_realized_pnl": 0.0,
				"total_unrealized_pnl": 0.0,
				"total_pnl": 0.0,
				"records": [],
			}

		positions = []
		open_tickers = []

		for ticker in tickers:
			pos = self.get_position(strat, ticker)
			if pos is None:
				continue

			positions.append(pos)
			if pos["num_shares"] > 0:
				open_tickers.append(ticker)

		price_map = self.get_market_prices(open_tickers, alpaca_api) if open_tickers else {}

		records = []
		total_realized = 0.0
		total_unrealized = 0.0
		open_positions = 0

		for pos in positions:
			ticker = pos["ticker"]
			side = pos["side"]
			num_shares = pos["num_shares"]
			avg_price_per_share = pos["avg_price_per_share"]
			realized_pnl = pos["realized_pnl"]
			last_market_price = pos["last_market_price"]

			if num_shares > 0:
				open_positions += 1
				market_price = price_map.get(ticker, {}).get("market")
				if market_price is not None:
					last_market_price = float(market_price)

				unrealized_pnl = self._compute_unrealized_pnl(
					side,
					avg_price_per_share,
					last_market_price,
					num_shares,
				)
			else:
				unrealized_pnl = 0.0

			total_pnl = realized_pnl + unrealized_pnl

			self.r.hset(
				self._position_key(strat, ticker),
				mapping={
					"last_market_price": self.tvw_helpers.to_str(last_market_price),
					"unrealized_pnl": self.tvw_helpers.to_str(unrealized_pnl),
					"realized_pnl": self.tvw_helpers.to_str(realized_pnl),
				},
			)

			fields = {
				"snapshot_time": snapshot_time,
				"strategy_name": strat,
				"ticker": ticker,
				"side": side,
				"num_shares": self.tvw_helpers.to_str(num_shares),
				"avg_price_per_share": self.tvw_helpers.to_str(avg_price_per_share),
				"last_market_price": self.tvw_helpers.to_str(last_market_price),
				"realized_pnl": self.tvw_helpers.to_str(realized_pnl),
				"unrealized_pnl": self.tvw_helpers.to_str(unrealized_pnl),
				"total_pnl": self.tvw_helpers.to_str(total_pnl),
			}

			self.r.xadd(
				self._pnl_stream_key(strat, ticker),
				fields,
				maxlen=self.pnl_stream_maxlen,
				approximate=True,
			)

			records.append({
				"ticker": ticker,
				"side": side,
				"num_shares": num_shares,
				"avg_price_per_share": avg_price_per_share,
				"last_market_price": last_market_price,
				"realized_pnl": realized_pnl,
				"unrealized_pnl": unrealized_pnl,
				"total_pnl": total_pnl,
			})

			total_realized += realized_pnl
			total_unrealized += unrealized_pnl

		aggregate_total = total_realized + total_unrealized
		aggregate_fields = {
			"snapshot_time": snapshot_time,
			"strategy_name": strat,
			"total_pnl": self.tvw_helpers.to_str(aggregate_total),
			"total_unrealized_pnl": self.tvw_helpers.to_str(total_unrealized),
			"total_realized_pnl": self.tvw_helpers.to_str(total_realized),
			"tracked_tickers": self.tvw_helpers.to_str(len(records)),
			"open_positions": self.tvw_helpers.to_str(open_positions),
		}

		self.r.xadd(
			self._aggregate_pnl_stream_key(strat),
			aggregate_fields,
			maxlen=self.pnl_stream_maxlen,
			approximate=True,
		)

		return {
			"strategy_name": strat,
			"snapshot_time": snapshot_time,
			"tracked_tickers": len(records),
			"open_positions": open_positions,
			"total_realized_pnl": total_realized,
			"total_unrealized_pnl": total_unrealized,
			"total_pnl": aggregate_total,
			"records": records,
		}

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

	def get_pnl_history(self, strategy_name: str, start: str = None, end: str = None, ticker: str = None):
		strat = self._normalize_strategy(strategy_name)
		sym = self._normalize_ticker(ticker) if ticker else None

		if sym:
			key = self._pnl_stream_key(strat, sym)
		else:
			key = self._aggregate_pnl_stream_key(strat)

		entries = self.r.xrange(key, min="-", max="+")
		history = []

		for entry_id, fields in entries:
			snapshot_time = fields.get("snapshot_time")
			if not snapshot_time:
				continue

			if not self._entry_in_range(snapshot_time, start, end):
				continue

			history.append({
				"id": entry_id,
				"snapshot_time": snapshot_time,
				"strategy_name": fields.get("strategy_name"),
				"ticker": fields.get("ticker"),
				"side": fields.get("side"),
				"num_shares": self._parse_float(fields.get("num_shares")),
				"avg_price_per_share": self._parse_float(fields.get("avg_price_per_share")),
				"last_market_price": self._parse_float(fields.get("last_market_price")),
				"realized_pnl": self._parse_float(fields.get("realized_pnl")),
				"unrealized_pnl": self._parse_float(fields.get("unrealized_pnl")),
				"total_pnl": self._parse_float(fields.get("total_pnl")),
				"tracked_tickers": self._parse_int(fields.get("tracked_tickers")),
				"open_positions": self._parse_int(fields.get("open_positions")),
				"total_unrealized_pnl": self._parse_float(fields.get("total_unrealized_pnl")),
				"total_realized_pnl": self._parse_float(fields.get("total_realized_pnl")),
			})

		return history

	def get_trade_events(self, strategy_name: str = None, ticker: str = None, start: str = None, end: str = None):
		"""
		Retrieves historical PnL snapshot data for a given strategy, optionally filtered
		by ticker and time range. Reads from the appropriate Redis stream (per-ticker or
		aggregate), filters entries by timestamp, and normalizes numeric fields for output.

		Parameters:
			strategy_name (str): The name of the trading strategy.
			start (str, optional): Inclusive start time boundary in ISO format.
			end (str, optional): Inclusive end time boundary in ISO format.
			ticker (str, optional): Specific ticker symbol to filter results; if not provided,
			returns aggregate strategy-level PnL.

		Returns:
			list[dict]: A list of PnL snapshot records matching the specified filters,
			each containing parsed numeric fields and associated metadata.
		"""		
		if strategy_name and ticker:
			key = self._trade_event_pair_stream_key(strategy_name, ticker)
		else:
			key = self._trade_event_stream_key()

		entries = self.r.xrange(key, min="-", max="+")
		events = []

		for entry_id, fields in entries:
			event_time = fields.get("event_time")
			if not event_time:
				continue

			if start or end:
				if not self._entry_in_range(event_time, start, end):
					continue

			if strategy_name and fields.get("strategy_name") != self._normalize_strategy(strategy_name):
				continue

			if ticker and fields.get("ticker") != self._normalize_ticker(ticker):
				continue

			events.append({
				"id": entry_id,
				"strategy_name": fields.get("strategy_name"),
				"ticker": fields.get("ticker"),
				"event_type": fields.get("event_type"),
				"entry_date": event_time,
				"price": self._parse_float(fields.get("price")),
				"num_shares": self._parse_float(fields.get("num_shares")),
				"side": fields.get("side"),
				"old_num_shares": self._parse_float(fields.get("old_num_shares")),
				"new_num_shares": self._parse_float(fields.get("new_num_shares")),
				"realized_delta": self._parse_float(fields.get("realized_delta")),
				"cumulative_realized_pnl": self._parse_float(fields.get("cumulative_realized_pnl")),
			})

		return events

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


	def get_first_trade_time(self):
		"""
		Retrieves the earliest trade timestamp from the global trade index stored in Redis.
		Reads the lowest-scored entry from the sorted set and converts its epoch score
		to an ISO-formatted UTC timestamp.

		Parameters:
			None

		Returns:
			Optional[str]: The ISO-formatted timestamp of the earliest recorded trade,
			or None if no trades exist.
		"""
		items = self.r.zrange(self._trade_index_key(), 0, 0, withscores=True)
		if not items:
			return None

		_, score = items[0]
		return datetime.fromtimestamp(score, timezone.utc).isoformat()		


	def get_last_trade_time(self):
		"""
		Retrieves the most recent trade timestamp from the global trade index stored in Redis.
		Reads the highest-scored entry from the sorted set and converts its epoch score
		to an ISO-formatted UTC timestamp.

		Parameters:
			None

		Returns:
			Optional[str]: The ISO-formatted timestamp of the most recent recorded trade,
			or None if no trades exist.
		"""	
		items = self.r.zrevrange(self._trade_index_key(), 0, 0, withscores=True)
		if not items:
			return None

		_, score = items[0]
		return datetime.fromtimestamp(score, timezone.utc).isoformat()	


	def get_trade_records_between(self, start_iso: str, end_iso: str, tickers=None):
		"""
		Retrieves records between the dates and tickers specified.
		Parameters:
			start_iso (str): Start date in iso format.
			end_iso (str): End date in iso format.
			tickers (list): List of tickers.
		Returns:
			records (dictionary): fields of each record matching the query.
		"""	
		try:
			start_ts = datetime.fromisoformat(start_iso.replace("Z", "+00:00")).timestamp()
			end_ts = datetime.fromisoformat(end_iso.replace("Z", "+00:00")).timestamp()
		except Exception as exc:
			raise ValueError("Invalid ISO date range") from exc

		if start_ts > end_ts:
			raise ValueError("start must be <= end")

		ticker_set = None
		if tickers:
			ticker_set = {str(t).upper().strip() for t in tickers if str(t).strip()}

		members = self.r.zrangebyscore(self._trade_index_key(), start_ts, end_ts)

		records = []
		for member in members:
			strat, ticker, stream_id = member.split("|", 2)
			ticker = ticker.upper().strip()

			if ticker_set is not None and ticker not in ticker_set:
				continue

			stream_key = self._trade_event_pair_stream_key(strat, ticker)

			entry = self.r.xrange(stream_key, min=stream_id, max=stream_id, count=1)
			if not entry:
				continue

			entry_id, fields = entry[0]
			records.append({
				"id": entry_id,
				"strategy_name": fields.get("strategy_name"),
				"ticker": fields.get("ticker"),
				"entry_date": fields.get("event_time"),
				"entry_price": self._parse_float(fields.get("price")),
				"num_shares": self._parse_float(fields.get("num_shares")),
				"side": fields.get("side"),
			})

		return records		

