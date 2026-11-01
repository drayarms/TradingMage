"""Separate strategy-owned books with one shared per-ticker direction guard."""
import copy
import numpy as np
import pandas as pd
from golden_cross_dip_recovery import STRATEGY_NAME
from range_strategy import range_entry_side, range_exit_reason, range_entry_fraction
from open_exposure_report import OpenExposureTracker

class StrategyPortfolio:
	def __init__(self,bt,settings,cross_state,frames,base):
		from backtester import SimState
		self.bt,self.settings,self.base=bt,settings,base
		self.books={'cross':cross_state,'range':SimState()}
		self.enabled={'cross':settings.cross_strategy_enabled,'range':settings.range_strategy_enabled}
		self.prices=cross_state.last_price_by_ticker
		# Price dictionaries are immutable within a run. Build UTC indexes once so
		# Eastern signal arrivals never scan a complete per-ticker minute history.
		price_index={}
		for ticker,prices in cross_state.market_data.get('close_1m',{}).items():
			times=np.array([pd.Timestamp(t).value for t in prices],dtype=np.int64)
			values=np.array(list(prices.values()),dtype=float)
			order=np.argsort(times)
			price_index[ticker]=(times[order],values[order])
		for book in self.books.values():
			book.market_data=cross_state.market_data
			book.last_price_by_ticker=self.prices
			book.execution_price_index=price_index
		self.aggregate=SimState(last_price_by_ticker=self.prices)
		self.cursors=dict.fromkeys(self.books,0)
		self.virtual_trade_events=[]
		self.diagnostics=[]
		self.exposure=OpenExposureTracker(frames)
		self.last_range_entry={}
		self.rejections=set()
		self.entry_claims={}
		self.rows={t:f.to_dict('records') for t,f in frames.items()}
		self.times={t:pd.DatetimeIndex(f.completed_at).asi8 for t,f in frames.items() if not f.empty}
		self.strategy_reports={owner:dict(enabled=enabled,overall_pnl_history=[]) for owner,enabled in self.enabled.items()}
		self.direction_reports={name:dict(overall_pnl_history=[]) for name in ('golden_cross','death_cross','range_long','range_short')}
		self.realized_directions=dict.fromkeys(self.direction_reports,0.)

	def active(self,owner):
		return self.enabled[owner] or self.settings.disabled_strategy_virtual_positions_enabled

	def allows(self,owner,ticker,side):
		if not self.active(owner): return False
		return not any(self.active(other) and ticker in book.positions and book.positions[ticker].side!=side for other,book in self.books.items())

	def prepare_entry(self,owner,ticker,side,event):
		"""Called only after sizing and all strategy entry requirements pass."""
		if not self.active(owner): return False
		if not self.bt.tvw_helpers.is_between_8pm_sun_and_8pm_fri_et(event['received_dt']): return False
		if event['received_dt'].date().isoformat() in self.books[owner].market_close_liquidation_dates: return False
		own=self.books[owner].positions.get(ticker)
		if own and own.side!=side: return False
		when=pd.Timestamp(event['received_dt']).value
		claim=self.entry_claims.get((ticker,when))
		if claim and claim[0]!=owner and claim[1]!=side: return False
		for other,book in self.books.items():
			position=book.positions.get(ticker)
			if other==owner or not self.active(other) or not position or position.side==side: continue
			self.track_exposure(event['received_dt'])
			reason='replaced_by_qualifying_'+owner+'_entry'
			if not self.bt._close_position(book,dict(event,exit_reason=reason)): return False
			book.trade_events[-1]['exit_reason']=reason
			book.last_exit_time_by_ticker[ticker]=event['received_dt']
			self.sync(other)
		return True

	def claim_entry(self,owner,ticker,side,event):
		self.entry_claims[ticker,pd.Timestamp(event['received_dt']).value]=(owner,side)

	def direction(self,owner,side):
		return ('golden_cross' if side=='long' else 'death_cross') if owner=='cross' else 'range_'+side

	def sync(self,owner):
		book=self.books[owner]
		for event in book.trade_events[self.cursors[owner]:]:
			event.update(strategy_type=owner,is_virtual=not self.enabled[owner])
			side='long' if event.get('side') in {'long','sell'} else 'short'
			event['strategy_direction']=self.direction(owner,side)
			if self.enabled[owner]:
				self.aggregate.trade_events.append(event)
				if event.get('event_type')=='close': self.realized_directions[event['strategy_direction']]+=float(event.get('realized_delta') or 0)
			else: self.virtual_trade_events.append(event)
		self.cursors[owner]=len(book.trade_events)

	def context(self,ticker,when):
		if ticker not in self.times: return None,None
		index=int(self.times[ticker].searchsorted(pd.Timestamp(when).value,side='right'))-1
		return (index,self.rows[ticker][index]) if index>=0 else (None,None)

	def range_event(self,ticker,when):
		when=pd.Timestamp(when).to_pydatetime().astimezone(self.bt.tvw_helpers.eastern_tz)
		return dict(ticker=ticker,dt=when,received_dt=when,sort_dt=when,time=when.isoformat(),received_time=when.isoformat(),timeframe='1m',signal='_range',raw_fields={},strategy_name=STRATEGY_NAME,exit_strategy=None)

	def process_range(self,ticker,when,entries=False):
		if not self.active('range'): return
		index,row=self.context(ticker,when)
		if row is None: return
		if entries and not row.get('ranging_market',False): return
		book=self.books['range']; position=book.positions.get(ticker)
		if not entries and position is None: return
		if entries and book.last_exit_time_by_ticker.get(ticker)==when: return
		event=self.range_event(ticker,when)
		try: quote=self.bt._get_simulated_quote(book,event)
		except ValueError: return
		market=float(quote['market'])
		if not entries:
			reason=range_exit_reason(position.side,market,self.rows[ticker],index)
			if reason:
				if self.bt._close_position(book,dict(event,exit_reason=reason)):
					book.last_exit_time_by_ticker[ticker]=when
					book.trade_events[-1]['exit_reason']=reason
				self.sync('range')
			return
		side=range_entry_side(market,row,self.settings.range_min_bollinger_gap_to_peak_since_turning_point_ratio)
		if side is None: return
		last=self.last_range_entry.get((ticker,side))
		if last is not None and index-last<2: return
		reason=None

		price=self.bt._get_execution_price(quote,'buy' if side=='long' else 'sell')
		fraction=range_entry_fraction(price,float(row['atr']),self.settings)
		exposure=position.num_shares*price if position else 0.
		amount=min(self.base*fraction,max(0.,self.base*self.settings.max_exposure_multiple-exposure))
		if amount<=0: reason=reason or 'exposure_cap_reached'
		if reason:
			key=(ticker,side,index,reason)
			if key not in self.rejections:
				self.diagnostics.append(dict(ticker=ticker,time=when.isoformat(),strategy_type='range',is_virtual=not self.enabled['range'],reason=reason))
				self.rejections.add(key)
			return
		if not self.prepare_entry('range',ticker,side,event): return
		previous=len(book.trade_events)
		self.bt._open_or_add_position(book,event,side,amount/price)
		trades=book.trade_events[previous:]
		if any(e.get('event_type') in {'open','add'} for e in trades):
			self.claim_entry('range',ticker,side,event)
			self.last_range_entry[ticker,side]=index
			for trade in trades:
				trade.update(entry_trigger='range_band_proximity',range_entry_fraction=fraction,atr_price_ratio=float(row['atr'])/price,range_context_completed_at=pd.Timestamp(row['completed_at']).isoformat())
		self.sync('range')

	def track_exposure(self,when):
		values={}
		for owner,book in self.books.items():
			if not self.enabled[owner]: continue
			for ticker,p in book.positions.items():
				values[ticker]=values.get(ticker,0.)+abs(p.num_shares*self.prices.get(ticker,p.avg_price_per_share))
		when=pd.Timestamp(when).to_pydatetime().astimezone(self.bt.tvw_helpers.eastern_tz)
		self.exposure.update(when,values)
		day=when.date().isoformat()
		self.aggregate.daily_max_exposure[day]=max(self.aggregate.daily_max_exposure.get(day,0.),sum(values.values()))

	def record(self,when):
		self.track_exposure(when)
		self.aggregate.positions={}
		self.aggregate.realized_by_ticker={}
		unrealized_directions=dict.fromkeys(self.direction_reports,0.)
		exposure_directions=dict.fromkeys(self.direction_reports,0.)
		for owner,book in self.books.items():
			if not self.enabled[owner]: continue
			for ticker,value in book.realized_by_ticker.items(): self.aggregate.realized_by_ticker[ticker]=self.aggregate.realized_by_ticker.get(ticker,0.)+value
			unrealized=exposure=0.
			for ticker,p in book.positions.items():
				price=self.prices.get(ticker,p.avg_price_per_share)
				value=(price-p.avg_price_per_share)*p.num_shares*(1 if p.side=='long' else -1)
				unrealized+=value; exposure+=abs(price*p.num_shares)
				direction=self.direction(owner,p.side)
				unrealized_directions[direction]+=value;exposure_directions[direction]+=abs(price*p.num_shares)
				combined=self.aggregate.positions.get(ticker)
				if combined:
					assert combined.side==p.side,'Opposite strategy-owned positions cannot coexist'
					quantity=combined.num_shares+p.num_shares
					combined.avg_price_per_share=(combined.avg_price_per_share*combined.num_shares+p.avg_price_per_share*p.num_shares)/quantity
					combined.num_shares=quantity
				else: self.aggregate.positions[ticker]=copy.copy(p)
			realized=sum(book.realized_by_ticker.values())
			report=self.strategy_reports[owner]
			report.update(realized_pnl=realized,unrealized_pnl=unrealized,total_pnl=realized+unrealized)
			report['overall_pnl_history'].append(dict(time=when.isoformat(),overall_total_pnl=realized+unrealized,gross_open_exposure=exposure))
		for direction,report in self.direction_reports.items():
			realized=self.realized_directions[direction];unrealized=unrealized_directions[direction]
			report.update(realized_pnl=realized,unrealized_pnl=unrealized,total_pnl=realized+unrealized)
			report['overall_pnl_history'].append(dict(time=when.isoformat(),overall_total_pnl=realized+unrealized,gross_open_exposure=exposure_directions[direction]))
		self.bt._record_snapshots(self.aggregate,when)

	def open_positions(self,virtual=False):
		rows=[]
		for owner,book in self.books.items():
			if (not self.enabled[owner])!=virtual: continue
			for ticker,p in book.positions.items():
				price=self.prices.get(ticker,p.avg_price_per_share)
				rows.append(dict(ticker=ticker,strategy_type=owner,is_virtual=virtual,side=p.side,num_shares=p.num_shares,avg_price_per_share=p.avg_price_per_share,market_price=price,unrealized_pnl=(price-p.avg_price_per_share)*p.num_shares*(1 if p.side=='long' else -1),market_value=price*p.num_shares))
		return rows
