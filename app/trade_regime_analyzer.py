"""Read-only diagnostics of actual simulated position episodes."""
from bisect import bisect_right
from collections import Counter, defaultdict
from datetime import timedelta
from statistics import mean, median
import math
import pandas as pd


class TradeRegimeAnalyzer:
	def analyze(self, trade_events, market_data, start_dt, end_dt, anchor_timeframe):
		"""Group by ENTRY week; reconstruct equity across adds/partial exits.

		MFE/MAE sample completed 1m closes plus fills, not intrabar high/low.
		Dollar excursions include realized partial-exit PnL plus open equity.
		ATR units use first entry ATR multiplied by first entry share quantity.
		"""
		tz = start_dt.tzinfo
		def dt(value):
			stamp = pd.Timestamp(value)
			return (stamp.tz_localize(tz) if stamp.tzinfo is None else stamp.tz_convert(tz)).to_pydatetime()

		def number(value):
			if value is None:
				return None
			v = float(value)
			return v if math.isfinite(v) else None

		def avg(values):
			v = [x for x in values if x is not None]
			return mean(v) if v else None

		prices, atrs = {}, {}
		for symbol, values in market_data.get('close_1m', {}).items():
			pairs = sorted((dt(t)+timedelta(minutes=1), number(p)) for t,p in values.items() if number(p) is not None and float(p)>0)
			prices[symbol] = ([t for t,p in pairs], [p for t,p in pairs])
		duration = timedelta(minutes={'15m':15, '1h':60, '4h':240}[anchor_timeframe])
		for symbol, values in market_data.get('anchor_atr', {}).items():
			pairs = sorted((dt(t)+duration, number(v)) for t,v in values.items() if number(v) is not None and float(v)>0)
			atrs[symbol] = ([t for t,v in pairs], [v for t,v in pairs])

		quality = Counter()
		active, completed = {}, []
		def mark(position, value):
			position['mfe'] = max(position['mfe'], value)
			position['mae'] = min(position['mae'], value)

		def sample(position, through):
			times, values = prices.get(position['ticker'], ([], []))
			a, b = bisect_right(times, position['last_time']), bisect_right(times, through)
			for index in range(a,b):
				p = values[index]
				mark(position, position['realized'] + position['sign']*(p-position['average'])*position['qty'])
			position['sample_count'] += b-a
			position['last_time'] = through

		# Stable order preserves close/reopen execution order at equal timestamps.
		events = [(dt(e.get('diagnostic_execution_time') or e['time']), i, e)
			for i,e in enumerate(trade_events) if e.get('event_type') in {'open','add','close'}]
		for time, _, event in sorted(events, key=lambda item:(item[0],item[1])):
			if time > end_dt:
				continue
			symbol, kind = event['ticker'], event['event_type']
			price, quantity = number(event.get('price')), number(event.get('num_shares'))
			if price is None or price<=0 or quantity is None or quantity<=0:
				quality['invalid_fill_count'] += 1
				continue
			position = active.get(symbol)
			if kind == 'open':
				if position is not None:
					quality['overlapping_open_count'] += 1
					continue
				side = event.get('side')
				if side not in {'long','short'}:
					quality['invalid_side_count'] += 1
					continue
				ats, avs = atrs.get(symbol, ([],[]))
				i = bisect_right(ats,time)-1
				atr = avs[i] if i>=0 and time-ats[i] <= timedelta(days=7) else None
				position = dict(ticker=symbol, side=side, sign=1 if side=='long' else -1,
					entry_time=time, last_time=time, initial_price=price, initial_qty=quantity,
					qty=quantity, average=price, entry_notional=price*quantity,
					entry_atr=atr, mfe=0.0, mae=0.0, realized=0.0, sample_count=0,
					scale_in_count=0, partial_exit_count=0, exit_reasons=[])
				active[symbol] = position
				continue
			if position is None:
				quality['unmatched_exit_count' if kind=='close' else 'unmatched_add_count'] += 1
				continue
			sample(position,time)
			mark(position, position['realized'] + position['sign']*(price-position['average'])*position['qty'])
			if kind == 'add':
				position['average'] = (position['average']*position['qty']+price*quantity)/(position['qty']+quantity)
				position['qty'] += quantity
				position['entry_notional'] += price*quantity
				position['scale_in_count'] += 1
				continue
			closed = number(event.get('closed_quantity'))
			if closed is None:
				closed = quantity
				quality['inferred_exit_quantity_count'] += 1
			if closed > position['qty']+1e-6:
				quality['inconsistent_exit_quantity_count'] += 1
				closed = position['qty']
			pnl = number(event.get('realized_delta'))
			position['realized'] += pnl if pnl is not None else position['sign']*(price-position['average'])*closed
			position['qty'] -= closed
			remaining = number(event.get('remaining_quantity'))
			if remaining is not None:
				position['qty'] = remaining
			position['exit_reasons'].append(event.get('exit_reason') or 'unspecified')
			mark(position, position['realized'] + position['sign']*(price-position['average'])*position['qty'])
			if position['qty']>1e-6:
				position['partial_exit_count'] += 1
				continue
			active.pop(symbol)
			if position['entry_time'] < start_dt:
				quality['prewindow_episode_count'] += 1
				continue
			entry_time = position['entry_time']
			atr = position['entry_atr']
			denominator = atr*position['initial_qty'] if atr else None
			observed = position['sample_count']>0
			mfe, mae = (position['mfe'],position['mae']) if observed else (None,None)
			completed.append(dict(
				ticker=symbol, side=position['side'],
				week=(entry_time.date()-timedelta(days=entry_time.weekday())).isoformat(),
				entry_time=entry_time.isoformat(),exit_time=time.isoformat(),
				holding_minutes=(time-entry_time).total_seconds()/60,
				entry_atr=atr,entry_atr_pct=atr/position['initial_price']*100 if atr else None,
				entry_notional=position['entry_notional'], pnl=position['realized'],
				return_pct=position['realized']/position['entry_notional']*100,
				mfe=mfe,mae=mae,
				mfe_pct=mfe/position['entry_notional']*100 if mfe is not None else None,
				mae_pct=mae/position['entry_notional']*100 if mae is not None else None,
				mfe_atr=mfe/denominator if mfe is not None and denominator else None,
				mae_atr=mae/denominator if mae is not None and denominator else None,
				giveback=mfe-position['realized'] if mfe is not None else None,
				scale_in_count=position['scale_in_count'],partial_exit_count=position['partial_exit_count'],
				price_sample_count=position['sample_count'],exit_reason=position['exit_reasons'][-1],
				exit_reasons=position['exit_reasons']))

		def aggregate(dimensions):
			groups = defaultdict(list)
			for trade in completed:
				groups[tuple(trade[k] for k in dimensions)].append(trade)
			rows = []
			for key, trades in sorted(groups.items()):
				wins = [t['pnl'] for t in trades if t['pnl']>0]
				losses = [t['pnl'] for t in trades if t['pnl']<0]
				row = dict(zip(dimensions,key))
				row.update(trade_count=len(trades),total_pnl=sum(t['pnl'] for t in trades),
					win_rate=len(wins)/len(trades),average_winner=avg(wins),average_loser=avg(losses),
					profit_factor=sum(wins)/abs(sum(losses)) if losses else None,
					median_holding_minutes=median(t['holding_minutes'] for t in trades),
					atr_sample_count=sum(t['entry_atr'] is not None for t in trades),
					excursion_sample_count=sum(t['mfe'] is not None for t in trades),
					scale_in_episode_count=sum(t['scale_in_count']>0 for t in trades),
					partial_exit_episode_count=sum(t['partial_exit_count']>0 for t in trades),
					exit_reason_counts=dict(Counter(t['exit_reason'] for t in trades)))
				for metric in ('pnl','return_pct','holding_minutes','entry_atr_pct','mfe','mae','mfe_pct','mae_pct','mfe_atr','mae_atr','giveback'):
					row['mean_'+metric] = avg(t[metric] for t in trades)
				rows.append(row)
			return rows

		quality['open_episode_count'] = len(active)
		quality['closed_episode_count'] = len(completed)
		quality['missing_excursion_episode_count'] = sum(t['mfe'] is None for t in completed)
		quality['missing_entry_atr_episode_count'] = sum(t['entry_atr'] is None for t in completed)
		return dict(metadata=dict(bucket='entry_week',anchor_timeframe=anchor_timeframe,
			notes=['MFE/MAE are equity at completed minute closes and fills, not intrabar extremes.',
				'Equity includes realized partial exits plus remaining position equity; adds use weighted cost basis.',
				'Return/excursion % denominator is total entry capital across all adds.',
				'ATR excursion denominator is first entry anchor ATR times first entry quantity.',
				'Only fully closed episodes with observed opens enter aggregates. Open/carryover positions are reported separately.',
				'Profit factor is null when there are no losing episodes; no breakpoint date is imposed.']),
			data_quality=dict(quality),weekly=aggregate(('week','side')),
			weekly_overall=aggregate(('week',)),weekly_by_ticker=aggregate(('week','ticker','side')),
			weekly_by_exit_reason=aggregate(('week','side','exit_reason')),
			trades=completed,open_episodes=[dict(ticker=p['ticker'],side=p['side'],entry_time=p['entry_time'].isoformat(),remaining_quantity=p['qty'],realized_pnl=p['realized']) for p in active.values()])
