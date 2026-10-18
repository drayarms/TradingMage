import logging
import math
from datetime import datetime, timedelta


logger = logging.getLogger("tv-webhook")


class PatternAnalyzer:
	"""Read-only multi-timeframe analysis shared by live and simulated strategies."""
	def __init__(self, trading_view_webhook_helpers):
		self.tvw_helpers = trading_view_webhook_helpers


	def is_confirmation_signal(self, fields):
		return str(
			fields.get("signal_role")
			or ""
		).strip().lower() == "confirmation"


	def tf_rank(self, timeframe):
		tf = self.tvw_helpers.normalize_tf(timeframe)
		order = {
			"1m": 1,
			"3m": 2,
			"5m": 3,
			"15m": 4,
			"30m": 5,
			"45m": 6,
			"1h": 7,
			"2h": 8,
			"4h": 9,
			"1d": 10,
		}
		return order.get(tf, -1)


	def latest_tf_signal_is_confirmation(
		self,
		history,
		ticker,
		timeframe,
	):
		last_alert = history.get_nth_last_alert(
			ticker,
			timeframe,
			1,
		)

		if last_alert is None:
			return False

		_, fields = last_alert
		return self.is_confirmation_signal(fields)


	def is_tf_relative_to_last_higher_tf(
		self,
		history,
		ticker,
		signal,
		timeframe,
		current_tf,
		higher_tf,
		relation="opposite",
	):
		tf = self.tvw_helpers.normalize_tf(timeframe)
		current_tf_norm = self.tvw_helpers.normalize_tf(current_tf)

		if tf != current_tf_norm:
			return False

		sym = str(ticker or "").upper().strip()
		current_side = self.tvw_helpers.normalize_signal(signal)

		if current_side not in {"buy", "sell"}:
			return False

		current_alert = history.get_nth_last_alert(
			sym,
			tf,
			1,
		)

		if current_alert is None:
			return False

		_, current_fields = current_alert

		if not self.is_confirmation_signal(current_fields):
			return False

		latest_higher_signal = (
			history.get_latest_confirmation_directional_signal(
				sym,
				higher_tf,
				max_scan=500,
			)
		)

		if latest_higher_signal is None:
			return False

		last_side = latest_higher_signal["side"]

		if relation == "opposite":
			return current_side != last_side

		if relation == "same":
			return current_side == last_side

		return False


	def get_latest_valid_same_side_signal(
		self,
		history,
		ticker,
		side,
		higher_tf,
		max_scan=500,
	):
		sym = str(ticker or "").upper().strip()
		target_side = self.tvw_helpers.normalize_signal(side)
		tf = self.tvw_helpers.normalize_tf(higher_tf)

		if target_side not in {"buy", "sell"}:
			return None

		if not tf:
			return None

		opposite_side = (
			"sell"
			if target_side == "buy"
			else "buy"
		)

		entries = history.get_recent_alerts(
			sym,
			tf,
			max_scan=max_scan,
		)

		for entry_id, fields in entries:
			if not self.is_confirmation_signal(fields):
				continue

			entry_side = self.tvw_helpers.normalize_signal(
				fields.get("signal")
			)

			if entry_side not in {"buy", "sell"}:
				continue

			if entry_side == opposite_side:
				return None

			if entry_side == target_side:
				return {
					"id": entry_id,
					"ticker": fields.get("symbol") or sym,
					"timeframe": self.tvw_helpers.normalize_tf(
						fields.get("timeframe")
						or tf
					),
					"signal": fields.get("signal"),
					"normalized_signal": entry_side,
					"bar_close_time_eastern": fields.get(
						"bar_close_time_eastern"
					),
					"open": self.tvw_helpers.safe_float(
						fields.get("open")
					),
					"high": self.tvw_helpers.safe_float(
						fields.get("high")
					),
					"low": self.tvw_helpers.safe_float(
						fields.get("low")
					),
					"close": self.tvw_helpers.safe_float(
						fields.get("close")
					),
					"volume": self.tvw_helpers.safe_float(
						fields.get("volume")
					),
					"price": self.tvw_helpers.safe_float(
						fields.get("price")
					),
				}

		return None


	def lower_tf_confirms_mid_tf_opposite_of_higher_tf(
		self,
		history,
		ticker,
		signal,
		timeframe,
		valid_lower_tfs,
		mid_tf,
		higher_tf,
	):
		tf = self.tvw_helpers.normalize_tf(timeframe)
		allowed_lower_tfs = {
			self.tvw_helpers.normalize_tf(item)
			for item in (valid_lower_tfs or [])
		}
		mid_tf_norm = self.tvw_helpers.normalize_tf(mid_tf)
		higher_tf_norm = self.tvw_helpers.normalize_tf(higher_tf)

		if tf not in allowed_lower_tfs:
			return False

		sym = str(ticker or "").upper().strip()
		current_side = self.tvw_helpers.normalize_signal(signal)

		if current_side not in {"buy", "sell"}:
			return False

		current_alert = history.get_nth_last_alert(sym, tf, 1)

		if current_alert is None:
			return False

		_, current_fields = current_alert

		if not self.is_confirmation_signal(current_fields):
			return False

		last_mid_signal = (
			history.get_latest_confirmation_directional_signal(
				sym,
				mid_tf_norm,
				max_scan=500,
			)
		)

		if last_mid_signal is None:
			return False

		last_higher_signal = (
			history.get_latest_confirmation_directional_signal(
				sym,
				higher_tf_norm,
				max_scan=500,
			)
		)

		if last_higher_signal is None:
			return False

		last_mid_side = last_mid_signal["side"]
		last_higher_side = last_higher_signal["side"]

		return (
			current_side == last_mid_side
			and last_mid_side != last_higher_side
		)


	def has_opposite_signal_since_last_valid_same_side_higher_tf(
		self,
		history,
		ticker,
		signal,
		opposite_tf,
		anchor_tf,
		max_scan_opposite_tf=1000,
		max_scan_anchor_tf=500,
	):
		sym = str(ticker or "").upper().strip()
		target_side = self.tvw_helpers.normalize_signal(signal)
		opposite_tf_norm = self.tvw_helpers.normalize_tf(opposite_tf)
		anchor_tf_norm = self.tvw_helpers.normalize_tf(anchor_tf)

		if target_side not in {"buy", "sell"}:
			return False

		if not opposite_tf_norm or not anchor_tf_norm:
			return False

		latest_anchor_signal = (
			history.get_latest_confirmation_directional_signal(
				sym,
				anchor_tf_norm,
				max_scan=max_scan_anchor_tf,
			)
		)

		if not latest_anchor_signal:
			return False

		if latest_anchor_signal["side"] != target_side:
			return False

		anchor_time_str = latest_anchor_signal["fields"].get(
			"bar_close_time_eastern"
		)

		try:
			anchor_time = datetime.fromisoformat(anchor_time_str)
		except Exception:
			return False

		opposite_side = (
			"sell"
			if target_side == "buy"
			else "buy"
		)

		for _, fields in history.get_recent_alerts(
			sym,
			opposite_tf_norm,
			max_scan=max_scan_opposite_tf,
		):
			if not self.is_confirmation_signal(fields):
				continue

			entry_side = self.tvw_helpers.normalize_signal(
				fields.get("signal")
			)

			if entry_side != opposite_side:
				continue

			entry_time_str = fields.get("bar_close_time_eastern")

			if not entry_time_str:
				continue

			try:
				entry_time = datetime.fromisoformat(entry_time_str)
			except Exception:
				continue

			if entry_time > anchor_time:
				return True

		return False


	def get_signal_based_progressive_entry_size(
		self,
		history,
		ticker,
		side,
		entry_tf,
		anchor_tf,
		base_num_shares,
		smallest_share_size,
		max_scan_entry_tf=1000,
		max_scan_anchor_tf=500,
	):
		sym = str(ticker or "").upper().strip()
		target_side = self.tvw_helpers.normalize_signal(side)
		entry_tf_norm = self.tvw_helpers.normalize_tf(entry_tf)
		anchor_tf_norm = self.tvw_helpers.normalize_tf(anchor_tf)

		if target_side not in {"buy", "sell"}:
			return 0.0

		try:
			base_num_shares = float(base_num_shares)
		except Exception:
			return 0.0

		if base_num_shares <= 0:
			return 0.0

		anchor_signal = (
			history.get_latest_confirmation_directional_signal(
				sym,
				anchor_tf_norm,
				max_scan=max_scan_anchor_tf,
			)
		)

		if not anchor_signal:
			return 0.0

		if anchor_signal["side"] != target_side:
			return 0.0

		anchor_time_str = anchor_signal["fields"].get(
			"bar_close_time_eastern"
		)

		if not anchor_time_str:
			return 0.0

		try:
			anchor_time = datetime.fromisoformat(anchor_time_str)
		except Exception:
			return 0.0

		qualifying_count = 0

		for _, fields in history.get_recent_alerts(
			sym,
			entry_tf_norm,
			max_scan=max_scan_entry_tf,
		):
			if not self.is_confirmation_signal(fields):
				continue

			entry_side = self.tvw_helpers.normalize_signal(
				fields.get("signal")
			)

			if entry_side not in {"buy", "sell"}:
				continue

			entry_time_str = fields.get("bar_close_time_eastern")

			if not entry_time_str:
				continue

			try:
				entry_time = datetime.fromisoformat(entry_time_str)
			except Exception:
				continue

			if entry_time < anchor_time:
				break

			if entry_side != target_side:
				continue

			qualifying_count += 1

		if qualifying_count < 1:
			return 0.0

		execution_qty = base_num_shares / (
			2 ** (qualifying_count - 1)
		)

		if target_side == "sell":
			execution_qty = math.floor(execution_qty)

		if execution_qty < smallest_share_size:
			return 0.0

		return execution_qty


	def is_latest_anchor_opposite_of_open_position(
		self,
		history,
		ticker,
		anchor_tf,
		alpaca_position_qty,
	):
		try:
			alpaca_position_qty = float(
				alpaca_position_qty
				or 0.0
			)
		except Exception:
			return False

		if alpaca_position_qty == 0:
			return False

		position_side = (
			"long"
			if alpaca_position_qty > 0
			else "short"
		)
		expected_anchor_side = (
			"buy"
			if position_side == "long"
			else "sell"
		)
		opposite_anchor_side = (
			"sell"
			if expected_anchor_side == "buy"
			else "buy"
		)

		latest_anchor_signal = (
			history.get_latest_confirmation_directional_signal(
				ticker,
				anchor_tf,
				max_scan=500,
			)
		)

		if latest_anchor_signal is None:
			return False

		return (
			latest_anchor_signal["side"]
			== opposite_anchor_side
		)


	def get_strategy4_anchor_alerts(
		self,
		history,
		ticker,
		anchor_tf,
		max_scan=500,
	):
		return history.get_recent_alerts_through_current_event(
			ticker=ticker,
			timeframe=anchor_tf,
			max_scan=max_scan,
		)


	def is_strategy4_confirmation(
		self,
		fields,
		expected_side,
	):
		if not self.is_confirmation_signal(fields):
			return False

		return (
			self.tvw_helpers.normalize_signal(
				fields.get("signal")
			)
			== expected_side
		)


	def strategy4_trend_entry_is_valid(
		self,
		alerts,
		entry_side,
	):
		if entry_side == "long":
			required_previous_exit = "bullish_exit"
			invalidating_exit = "bearish_exit"
		elif entry_side == "short":
			required_previous_exit = "bearish_exit"
			invalidating_exit = "bullish_exit"
		else:
			return False

		for _, fields in alerts[1:]:
			historical_signal = str(
				fields.get("signal")
				or ""
			).strip().lower()

			if historical_signal == invalidating_exit:
				return False

			if historical_signal == required_previous_exit:
				return True

		return False


	def strategy4_range_entry_is_valid(
		self,
		alerts,
		entry_side,
	):
		if entry_side == "short":
			confirmation_side = "sell"
			required_exit = "bearish_exit"
		elif entry_side == "long":
			confirmation_side = "buy"
			required_exit = "bullish_exit"
		else:
			return False

		matching_exit_count = 0

		for _, fields in alerts[1:]:
			historical_signal = str(
				fields.get("signal")
				or ""
			).strip().lower()

			if historical_signal == required_exit:
				matching_exit_count += 1

				if matching_exit_count > 1:
					return False

			if self.is_strategy4_confirmation(
				fields,
				confirmation_side,
			):
				return matching_exit_count == 1

		return False


	def get_strategy4_entry_side(
		self,
		history,
		ticker,
		anchor_tf,
		signal,
	):
		alerts = self.get_strategy4_anchor_alerts(
			history=history,
			ticker=ticker,
			anchor_tf=anchor_tf,
			max_scan=500,
		)

		if not alerts:
			return None

		_, current_fields = alerts[0]
		current_signal = str(signal or "").strip().lower()
		stored_current_signal = str(
			current_fields.get("signal")
			or ""
		).strip().lower()

		if current_signal != stored_current_signal:
			return None

		normalized_side = self.tvw_helpers.normalize_signal(
			current_signal
		)

		if (
			normalized_side == "buy"
			and self.is_strategy4_confirmation(
				current_fields,
				"buy",
			)
		):
			if self.strategy4_trend_entry_is_valid(
				alerts,
				"long",
			):
				return "long"

			return None

		if (
			normalized_side == "sell"
			and self.is_strategy4_confirmation(
				current_fields,
				"sell",
			)
		):
			if self.strategy4_trend_entry_is_valid(
				alerts,
				"short",
			):
				return "short"

			return None

		return None


	def analyze_signal_regimes(
		self, events, market_data, start_dt, end_dt,
		timeframes=("1m", "5m", "15m", "1h", "4h"),
		forward_horizons_minutes=(15, 30, 60, 120, 240),
		whipsaw_windows_minutes=(30, 60, 120, 240),
		bucket="week", ATR_period=14, context_max_age_minutes=240,
		propagation_window_minutes=180, price_tolerance_seconds=90,
	):
		"""Read-only, arrival-time research. All return values use percentage points.

		Price bars are timestamped at START; closes become available one minute
		later. Horizons are wall-clock minutes within regular Eastern sessions.
		Missing targets are censored, never interpolated or carried overnight.
		This measures gross signal behavior, not executable strategy returns.
		"""
		from bisect import bisect_left, bisect_right
		from collections import defaultdict
		from statistics import mean, median
		import pandas as pd

		if bucket != "week":
			raise ValueError("bucket must be week")
		if start_dt.tzinfo is None or end_dt.tzinfo is None or start_dt >= end_dt:
			raise ValueError("start must precede end; datetimes must be timezone-aware")
		timeframes = list(dict.fromkeys(self.tvw_helpers.normalize_tf(t) for t in timeframes))
		if not timeframes or set(timeframes) - {"1m", "5m", "15m", "1h", "4h"}:
			raise ValueError("timeframes must contain only 1m,5m,15m,1h,4h")
		horizons = sorted(set(int(x) for x in forward_horizons_minutes))
		windows = sorted(set(int(x) for x in whipsaw_windows_minutes))
		if not horizons or not windows or min(horizons + windows) < 1:
			raise ValueError("horizons and whipsaw windows must be positive minutes")
		if min(ATR_period, context_max_age_minutes, propagation_window_minutes, price_tolerance_seconds) < 1:
			raise ValueError("ATR period, context age, propagation window and tolerance must be positive")
		tz = self.tvw_helpers.eastern_tz
		start_dt, end_dt = start_dt.astimezone(tz), end_dt.astimezone(tz)

		def eastern(value):
			dt = pd.Timestamp(value)
			return (dt.tz_localize(tz) if dt.tzinfo is None else dt.tz_convert(tz)).to_pydatetime()

		def regular(dt):
			calendar = market_data.get("session_closes")
			if calendar is not None:
				close = calendar.get(dt.date().isoformat())
				if close is None or dt >= eastern(close):
					return False
			minute = dt.hour * 60 + dt.minute
			return dt.weekday() < 5 and 570 <= minute < 960

		def week(dt):
			return (dt.date() - timedelta(days=dt.weekday())).isoformat()

		def average(values):
			values = [float(v) for v in values if v is not None and math.isfinite(float(v))]
			return mean(values) if values else None

		# Index all retained context first. Reporting filters do not discard warmup.
		groups = defaultdict(list)
		quality = defaultdict(int)
		for raw in events:
			if raw.get("side") not in {"buy", "sell"} or raw.get("timeframe") not in timeframes:
				quality["non_directional_or_unselected_events"] += 1
				continue
			dt = eastern(raw.get("received_dt") or raw["dt"])
			if dt > end_dt:
				continue
			if not regular(dt):
				quality["outside_regular_session_events"] += int(dt >= start_dt)
				continue
			e = dict(raw, analysis_dt=dt)
			groups[(e["ticker"], e["timeframe"])].append(e)
		for key in groups:
			groups[key].sort(key=lambda e: (e["analysis_dt"], str(e.get("stream_id", ""))))
		event_times = {k: [e["analysis_dt"].timestamp() for e in v] for k, v in groups.items()}
		# Linear backward pass: next opposite event, avoiding O(N^2) history scans.
		next_opposite = {}
		for key, history in groups.items():
			next_side = {}
			for i in range(len(history) - 1, -1, -1):
				e = history[i]
				opp = next_side.get("sell" if e["side"] == "buy" else "buy")
				next_opposite[(key, i)] = opp
				next_side[e["side"]] = e["analysis_dt"]

		prices = {}
		environment = []
		for symbol, closes in market_data.get("close_1m", {}).items():
			bars = []
			for stamp, close in closes.items():
				source = eastern(stamp)
				values = [market_data.get(name, {}).get(symbol, {}).get(stamp) for name in ("open_1m", "high_1m", "low_1m")]
				if not regular(source) or close is None or not math.isfinite(float(close)) or float(close) <= 0:
					continue
				if any(v is None or not math.isfinite(float(v)) or float(v) <= 0 for v in values):
					quality["invalid_ohlc_bars"] += 1
					continue
				bars.append((source + timedelta(minutes=1), float(close), *map(float, values)))
			bars.sort(key=lambda b: b[0])
			if not bars:
				continue
			df = pd.DataFrame(bars, columns=["dt", "close", "open", "high", "low"]).set_index("dt")
			df = df[~df.index.duplicated(keep="last")]
			previous = df.close.shift(1)
			tr = pd.concat([df.high-df.low, (df.high-previous).abs(), (df.low-previous).abs()], axis=1).max(axis=1)
			# Explicit simple ATR on 1m bars, not the strategies' anchor Wilder ATR.
			df["atr_pct"] = tr.rolling(ATR_period, min_periods=ATR_period).mean() / df.close * 100
			prices[symbol] = (list(df.index.to_pydatetime()), df)
			for day, day_df in df.groupby(df.index.date):
				if not (start_dt.date() <= day <= end_dt.date()):
					continue
				day_df = day_df[(day_df.index >= start_dt) & (day_df.index <= end_dt)]
				if len(day_df) < 2:
					continue
				first, last = float(day_df.open.iloc[0]), float(day_df.close.iloc[-1])
				path = float(day_df.close.diff().abs().sum()) + abs(float(day_df.close.iloc[0])-first)
				environment.append({
					"week": week(day_df.index[0]), "ticker": symbol, "date": day.isoformat(),
					"bar_count": len(day_df),
					"observed_start": day_df.index[0].isoformat(), "observed_end": day_df.index[-1].isoformat(),
					"intraday_range_pct": float((day_df.high.max()-day_df.low.min())/first*100),
					"realized_volatility_pct": float((day_df.close.pct_change().dropna()**2).sum()**.5*100),
					"directional_efficiency": abs(last-first)/path if path else 0.0,
					"atr_1m_pct": average(day_df.atr_pct),
				})

		def price_at(symbol, dt, target=False):
			indexed = prices.get(symbol)
			if indexed is None:
				return None
			times, df = indexed
			i = bisect_right(times, dt) - 1
			age_limit = min(price_tolerance_seconds, 59.999999) if target else price_tolerance_seconds
			if i < 0 or times[i].date() != dt.date() or (dt-times[i]).total_seconds() > age_limit:
				return None
			return (float(df.close.iloc[i]), i, df)

		samples = []
		agreement_pairs = defaultdict(list)
		propagation_samples = []
		for key, history in groups.items():
			for i, e in enumerate(history):
				dt = e["analysis_dt"]
				if dt < start_dt:
					continue
				symbol, tf = key
				session_end = eastern(market_data["session_closes"][dt.date().isoformat()]) if "session_closes" in market_data else dt.replace(hour=16, minute=0, second=0, microsecond=0)
				observed_end = min(session_end, end_dt)
				sign = 1 if e["side"] == "buy" else -1
				base = price_at(symbol, dt)
				r = {"week": week(dt), "ticker": symbol, "timeframe": tf,
					"direction": e["side"], "signal": e.get("signal", e["side"]),
					"signal_role": e.get("signal_role", ""), "atr_pct": None,
					"agreement": None, "known_tf": 0, "minutes_to_opposite": None}
				if base is None:
					quality["signals_missing_base_price"] += 1
				else:
					r["atr_pct"] = average([base[2].atr_pct.iloc[base[1]]])
				for h in horizons:
					target = dt + timedelta(minutes=h)
					p = price_at(symbol, target, target=True) if target <= observed_end else None
					r[f"return_{h}"] = sign * (p[0]/base[0]-1)*100 if base and p else None
				opp = next_opposite[(key, i)]
				if opp and opp <= observed_end:
					r["minutes_to_opposite"] = (opp-dt).total_seconds()/60
				for w in windows:
					limit = dt + timedelta(minutes=w)
					hit = opp is not None and dt <= opp <= min(limit, observed_end)
					r[f"whipsaw_{w}"] = int(hit) if hit or limit <= observed_end else None
				agreements = []
				for other_tf in timeframes:
					if other_tf == tf:
						continue
					other_key = (symbol, other_tf)
					# Strictly earlier arrival times: simultaneous events cannot leak context.
					j = bisect_left(event_times.get(other_key, []), dt.timestamp())-1
					if j < 0:
						continue
					other = groups[other_key][j]
					age = (dt-other["analysis_dt"]).total_seconds()/60
					if other["analysis_dt"].date() == dt.date() and age <= context_max_age_minutes:
						agreement = int(other["side"] == e["side"])
						agreements.append(agreement)
						agreement_pairs[(week(dt), tf, e["side"], other_tf)].append(agreement)
				r["known_tf"] = len(agreements)
				r["agreement"] = average(agreements)
				samples.append(r)

				# Ordered confirmation chain; abort on first opposing directional alert
				# in the next stage or a reversal of the seed timeframe.
				chains = {"1m": ("1m", "5m", "15m", "1h"), "5m": ("5m", "15m", "1h"), "15m": ("15m", "1h")}
				chain = chains.get(tf)
				if chain and set(chain).issubset(timeframes):
					limit = dt + timedelta(minutes=propagation_window_minutes)
					deadline = min(limit, observed_end)
					cursor, complete, contradicted = dt, True, False
					if opp and opp <= deadline:
						deadline = opp
					for next_tf in chain[1:]:
						nk = (symbol, next_tf)
						j = bisect_right(event_times.get(nk, []), cursor.timestamp())
						if j >= len(groups.get(nk, [])) or groups[nk][j]["analysis_dt"] > deadline:
							complete = False
							break
						candidate = groups[nk][j]
						if candidate["side"] != e["side"] or (opp and candidate["analysis_dt"] >= opp):
							complete, contradicted = False, True
							break
						cursor = candidate["analysis_dt"]
					eligible = complete or contradicted or bool(opp and opp <= observed_end) or limit <= observed_end
					propagation_samples.append(dict(r, chain="->".join(chain),
						completed=int(complete) if eligible else None,
						completion_minutes=(cursor-dt).total_seconds()/60 if complete else None))

		def aggregate(rows, dimensions):
			buckets = defaultdict(list)
			for r in rows:
				buckets[tuple(r[d] for d in dimensions)].append(r)
			output = []
			for key, values in sorted(buckets.items()):
				row = dict(zip(dimensions, key))
				row.update(signal_count=len(values), ticker_count=len({r["ticker"] for r in values}),
					cross_tf_agreement=average(r["agreement"] for r in values),
					agreement_sample_count=sum(r["agreement"] is not None for r in values),
					mean_known_other_timeframes=average(r["known_tf"] for r in values),
					atr_pct=average(r["atr_pct"] for r in values),
					atr_sample_count=sum(r["atr_pct"] is not None for r in values),
					mean_minutes_to_opposite=average(r["minutes_to_opposite"] for r in values),
					observed_opposite_count=sum(r["minutes_to_opposite"] is not None for r in values))
				for h in horizons:
					v = [r[f"return_{h}"] for r in values if r[f"return_{h}"] is not None]
					ticker_returns = defaultdict(list)
					for r in values:
						if r[f"return_{h}"] is not None:
							ticker_returns[r["ticker"]].append(r[f"return_{h}"])
					means = [mean(x) for x in ticker_returns.values()]
					row.update({f"valid_return_count_{h}m": len(v), f"missing_return_count_{h}m": len(values)-len(v),
						f"mean_directional_return_{h}m": average(v), f"median_directional_return_{h}m": median(v) if v else None,
						f"directional_accuracy_{h}m": average(int(x > 0) for x in v),
						f"zero_return_count_{h}m": sum(x == 0 for x in v),
						f"reverse_expectancy_{h}m": -mean(v) if v else None,
						f"ticker_equal_weight_return_{h}m": average(means),
						f"ticker_breadth_{h}m": average(int(x > 0) for x in means),
						f"valid_ticker_count_{h}m": len(means)})
				for w in windows:
					v = [r[f"whipsaw_{w}"] for r in values if r[f"whipsaw_{w}"] is not None]
					row[f"whipsaw_rate_{w}m"] = average(v)
					row[f"valid_whipsaw_count_{w}m"] = len(v)
					row[f"censored_whipsaw_count_{w}m"] = len(values)-len(v)
				if "chain" in dimensions:
					row["completion_rate"] = average(r["completed"] for r in values)
					row["eligible_seed_count"] = sum(r["completed"] is not None for r in values)
					row["mean_completion_minutes"] = average(r["completion_minutes"] for r in values)
					for h in horizons:
						completed = [r[f"return_{h}"] for r in values if r["completed"] == 1]
						failed = [r[f"return_{h}"] for r in values if r["completed"] == 0]
						row[f"completed_seed_return_{h}m"] = average(completed)
						row[f"failed_seed_return_{h}m"] = average(failed)
				output.append(row)
			return output

		env_weeks = defaultdict(list)
		for row in environment:
			env_weeks[row["week"]].append(row)
		weekly_environment = []
		for w, rows in sorted(env_weeks.items()):
			weekly_environment.append(dict(week=w, ticker_day_count=len(rows),
				**{name: average(r[name] for r in rows) for name in
				("intraday_range_pct", "realized_volatility_pct", "directional_efficiency", "atr_1m_pct")}))
		quality["reported_directional_events"] = len(samples)
		quality["symbols_with_prices"] = len(prices)
		quality["symbols_without_prices"] = sorted({r["ticker"] for r in samples} - set(prices))
		return {
			"metadata": {"start": start_dt.isoformat(), "end": end_dt.isoformat(), "bucket": bucket,
				"timeframes": timeframes, "forward_horizons_minutes": horizons, "whipsaw_windows_minutes": windows,
				"ATR_period": ATR_period, "atr_definition": "simple mean true range of completed 1m bars / close * 100",
				"return_units": "percent (1.0 means 1%)", "rate_units": "fraction (0.5 means 50%)",
				"signal_clock": "received_dt (arrival)", "session": "Regular America/New_York session; official early closes when calendar supplied",
				"context_max_age_minutes": context_max_age_minutes, "propagation_window_minutes": propagation_window_minutes,
				"price_tolerance_seconds": price_tolerance_seconds,
				"notes": ["No automatic breakpoint detection or strategy changes.",
					"Gross signed price returns; reverse expectancy is arithmetic negation, not a reversed trade backtest.",
					"Persistence mean includes only observed same-session reversals; inspect censor counts.",
					"Early closes and gaps yield missing returns; environment covers observed bars, including partial days.",
					"Propagation returns are measured from the seed; completion is a future research label, not an entry rule.",
					"Overlapping signals are dependent observations; counts are not independent sample sizes."]},
			"data_quality": dict(quality),
			"weekly": aggregate(samples, ("week", "timeframe", "direction")),
			"weekly_overall": aggregate(samples, ("week",)),
			"weekly_by_signal": aggregate(samples, ("week", "timeframe", "direction", "signal", "signal_role")),
			"weekly_by_ticker": aggregate(samples, ("week", "ticker", "timeframe", "direction")),
			"cross_timeframe_agreement": [dict(week=k[0], timeframe=k[1], direction=k[2], other_timeframe=k[3],
				known_context_count=len(v), agreement_rate=average(v)) for k,v in sorted(agreement_pairs.items())],
			"propagation": aggregate(propagation_samples, ("week", "chain", "direction")),
			"weekly_environment": weekly_environment, "ticker_day_environment": environment,
		}

