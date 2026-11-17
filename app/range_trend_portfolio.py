"""Independent virtual inventory books, with family-level precedence."""
import pandas as pd
from strategy_portfolio import StrategyPortfolio
from range_trend_settings import STRATEGY_NAME
from trend_strategy import entry_reason,exit_reason,observe_band_visit
import math
from capital_exposure_report import NetAndLedgerExposureTracker,position_exposures

class RangeTrendPortfolio(StrategyPortfolio):
	def __init__(self,bt,settings,state,frames,base):
		from backtester import SimState
		super().__init__(bt,settings,state,frames,base)
		self.enabled['cross']=False
		for side in ('long','short'):
			owner='trend_'+side;book=SimState();book.market_data=state.market_data;book.last_price_by_ticker=self.prices;book.execution_price_index=state.execution_price_index
			self.books[owner]=book;self.enabled[owner]=settings.trend_strategy_enabled;self.cursors[owner]=0
			book.exit_allowed=lambda event:True
			book.on_position_closed=lambda ticker,when,owner=owner:self.position_closed(owner,ticker,when)
			book.on_entry_filled=lambda ticker,side,when,owner=owner:self.trend_filled(owner,ticker,when)
			book.defer_exit=lambda event,side,owner=owner:self.limit_orders.submit(owner,event,side,exit_order=True)
			book.defer_closed_exit=lambda event,owner=owner:self.limit_orders.defer_closed_exit(owner,event)
		self.last_trend_reversal={};self.trend_band_visits={}
		self.exposure=NetAndLedgerExposureTracker(frames)
		self.aggregate.daily_max_exposure=self.exposure.daily_net
		self.strategy_reports={k:dict(enabled=v,overall_pnl_history=[]) for k,v in self.enabled.items() if k!='cross'}
		self.direction_reports={k:dict(overall_pnl_history=[]) for k in ('trend_long','trend_short','range_long','range_short','post_stretch_range_long','post_stretch_range_short')}
		self.realized_directions=dict.fromkeys(self.direction_reports,0.)
		from range_trend_orders import RangeTrendLimitOrders
		self.limit_orders=RangeTrendLimitOrders(self)
	@staticmethod
	def family(owner):return 'trend' if owner.startswith('trend_') else 'range'
	def active(self,owner):return owner!='cross' and (self.enabled[owner] or self.settings.disabled_strategy_virtual_positions_enabled)
	def direction(self,owner,side):return owner if owner.startswith('trend_') else owner+'_'+side
	def range_event(self,ticker,when):return dict(super().range_event(ticker,when),strategy_name=STRATEGY_NAME)
	def range_exposure(self,ticker,price,side=None):
		return sum(book.positions[ticker].num_shares*price for owner,book in self.books.items() if owner in {'range','post_stretch_range'} and self.active(owner) and ticker in book.positions and (side is None or book.positions[ticker].side==side))
	def entry_zone_reason(self,side,ticker,when,owner='range'):
		if owner.startswith('trend_'):return None
		return super().entry_zone_reason(side,ticker,when,owner)
	def exit_allowed(self,owner,event):
		if owner.startswith('trend_') or event.get('exit_reason','').startswith('replaced_by_qualifying_'):return True
		return super().exit_allowed(owner,event)
	def process_heikin_ashi_exits(self,ticker,when):
		# Legacy 1h filters apply only to the two range books.
		from heikin_ashi_filter import EXIT_REASON
		from golden_cross_dip_recovery import HEIKIN_ASHI_FILTER_FLAGS
		for owner in ('range','post_stretch_range'):
			book=self.books[owner];position=book.positions.get(ticker)
			if not self.active(owner) or position is None or not getattr(self.settings,HEIKIN_ASHI_FILTER_FLAGS[owner]['exit']):continue
			if self.heikin_ashi_runs.blocks(ticker,position.side,when):
				if self.bt._close_position(book,dict(self.range_event(ticker,when),exit_reason=EXIT_REASON)):self.sync(owner)
	def process_zone_exits(self,ticker,when,record_zone=False):
		# Transition exits remain range-only. Trend exits use their own policy.
		trend_positions={o:b.positions for o,b in self.books.items() if o.startswith('trend_')}
		try:
			for o in trend_positions:self.books[o].positions={}
			super().process_zone_exits(ticker,when,record_zone)
		finally:
			for o,positions in trend_positions.items():self.books[o].positions=positions
	def precedence_allowed(self,owner,ticker,event):
		preferred=self.settings.entry_strategy_precedence;family=self.family(owner)
		if preferred=='none':return True
		others=[(o,b) for o,b in self.books.items() if self.active(o) and self.family(o)!=family and self.enabled[o]==self.enabled[owner]]
		if family!=preferred:return not any(ticker in b.positions or (o,ticker) in self.limit_orders.pending and not self.limit_orders.pending[o,ticker]['exit_order'] for o,b in others)
		for other,book in others:
			pending=self.limit_orders.pending.get((other,ticker))
			if pending and not pending['exit_order']:self.limit_orders.finish((other,ticker),event['received_dt'],'cancelled_by_preferred_entry')
			if ticker in book.positions:
				clean={k:v for k,v in event.items() if k not in {'_limit_fill','_execution_fill_price'}}
				if event.get('_limit_fill'):
					self.bt._close_position(book,dict(clean,exit_reason='replaced_by_qualifying_'+family+'_entry'))
					self.sync(other)
					return False # Reconsider the preferred entry after the independent exit fills.
				if not self.bt._close_position(book,dict(clean,exit_reason='replaced_by_qualifying_'+family+'_entry')):return False
				book.trade_events[-1]['exit_reason']='replaced_by_qualifying_'+family+'_entry'
				self.sync(other)
		return True
	def prepare_entry(self,owner,ticker,side,event):
		if not self.active(owner) or not self.bt._execution_session_allowed(self.books[owner],event['received_dt']):return False
		if (owner,ticker) in self.limit_orders.pending and not event.get('_limit_fill'):return False
		if self.books[owner].last_exit_time_by_ticker.get(ticker)==event['received_dt']:return False
		own=self.books[owner].positions.get(ticker)
		if own and own.side!=side:return False
		if owner.startswith('trend_'):
			_,row=self.context(ticker,event['received_dt']);price=event.get('_decision_price',event.get('_execution_fill_price',event.get('price')))
			if self.trend_exit_reason(owner,ticker,price,row):return False
			decision=entry_reason(side,price,row,own is not None,self.last_trend_reversal.get((owner,ticker)),cooldown_candles=self.settings.trend_entry_opposing_universal_zone_cooldown_candles)
			if not decision:return False
			# A pending fill must still belong to the same completed reversal candle.
			if event.get('_trend_reversal_id') and decision[1]!=event['_trend_reversal_id']:return False
		else:
			if self.entry_zone_reason(side,ticker,event['received_dt'],owner):return False
			index,row=self.context(ticker,event['received_dt']);last=self.last_range_entry.get((ticker,side))
			if last is not None and (index is None or index-last<2):return False
			if row is None or not row.get('ranging_market',False):return False
			if owner=='range' and event.get('_limit_fill'):
				from range_strategy import range_entry_side
				if range_entry_side(event.get('_execution_fill_price',event.get('price')),row,self.settings.range_min_bollinger_gap_to_peak_since_turning_point_ratio,self.settings.bollinger_proximity_atr_multiple)!=side:return False
			if owner=='post_stretch_range':
				from range_strategy import post_stretch_entry_side
				if row is None or post_stretch_entry_side(event.get('_execution_fill_price',event.get('price',float('nan'))),row)!=side:return False
		return self.precedence_allowed(owner,ticker,event)
	def claim_entry(self,owner,ticker,side,event):pass # Independent books may fill on the same minute.
	def position_closed(self,owner,ticker,when):
		super().position_closed(owner,ticker,when)
		self.trend_band_visits.pop((owner,ticker),None)
	def trend_quantity(self,owner,ticker,price):
		position=self.books[owner].positions.get(ticker)
		count=position.entry_sequence_count if position else 0
		return math.ldexp(self.base,-count)/price
	def trend_exit_reason(self,owner,ticker,price,row,observe=False):
		side=owner.removeprefix('trend_');visit=self.trend_band_visits.get((owner,ticker))
		if observe and ticker in self.books[owner].positions:
			visit=self.trend_band_visits.setdefault((owner,ticker),dict(proximity=self.settings.bollinger_proximity_atr_multiple))
			visit['proximity']=self.settings.bollinger_proximity_atr_multiple
			observe_band_visit(side,price,row,visit)
		return exit_reason(side,price,row,self.settings.trend_exit_bollinger_atr_multiple,self.settings.trend_exit_bollinger_gap_contraction_divisor,self.settings.bollinger_proximity_atr_multiple,visit)
	def trend_filled(self,owner,ticker,when):
		_,row=self.context(ticker,when)
		if row:
			self.last_trend_reversal[owner,ticker]=row.get(owner+'_reversal_id')
			# Begin visit tracking at the first actual fill, without resetting on additions.
			try:price=float(self.bt._get_simulated_quote(self.books[owner],self.range_event(ticker,when))['market'])
			except ValueError:return
			visit=self.trend_band_visits.setdefault((owner,ticker),dict(proximity=self.settings.bollinger_proximity_atr_multiple))
			observe_band_visit(owner.removeprefix('trend_'),price,row,visit)
	def process_trend(self,ticker,when,entries=False):
		_,row=self.context(ticker,when)
		if row is None:return
		for side in ('long','short'):
			owner='trend_'+side
			if not self.active(owner):continue
			book=self.books[owner];position=book.positions.get(ticker);event=self.range_event(ticker,when)
			try:quote=self.bt._get_simulated_quote(book,event);market=float(quote['market'])
			except ValueError:continue
			if not entries:
				if position and (reason:=self.trend_exit_reason(owner,ticker,market,row,observe=True)):
					if self.bt._close_position(book,dict(event,exit_reason=reason)):
						book.trade_events[-1]['exit_reason']=reason;self.sync(owner)
				continue
			if self.settings.strategy_mode not in {'both','golden_cross' if side=='long' else 'death_cross'}:continue
			price=self.bt._get_execution_price(quote,'buy' if side=='long' else 'sell');event['price']=price;event['_decision_price']=market
			if self.trend_exit_reason(owner,ticker,market,row):continue
			decision=entry_reason(side,market,row,position is not None,self.last_trend_reversal.get((owner,ticker)),cooldown_candles=self.settings.trend_entry_opposing_universal_zone_cooldown_candles)
			if not decision:continue
			qty=self.trend_quantity(owner,ticker,price)
			if qty<=0:continue
			event.update(entry_trigger=decision[0],_trend_reversal_id=decision[1])
			if not self.prepare_entry(owner,ticker,side,event):continue
			if self.limit_orders.submit(owner,event,side,qty):continue
			if self.bt._open_or_add_position(book,event,side,qty):self.sync(owner)
	def sync(self,owner):
		book=self.books[owner]
		for event in book.trade_events[self.cursors[owner]:]:
			position_side=event.get('position_side') or ('long' if event.get('side') in {'long','sell'} else 'short')
			event.update(strategy_type='trend' if owner.startswith('trend_') else owner,position_side=position_side,is_virtual=not self.enabled[owner],strategy_direction=self.direction(owner,position_side))
			if self.enabled[owner]:
				self.aggregate.trade_events.append(event)
				if event.get('event_type')=='close':self.realized_directions[event['strategy_direction']]+=float(event.get('realized_delta') or 0.)
			else:self.virtual_trade_events.append(event)
		self.cursors[owner]=len(book.trade_events)
	def open_positions(self,virtual=False):
		return [dict(row,strategy_type='trend' if row['strategy_type'].startswith('trend_') else row['strategy_type']) for row in super().open_positions(virtual)]
	def track_exposure(self,when):
		snapshot=position_exposures(self.books,self.enabled,self.prices)
		when=pd.Timestamp(when).to_pydatetime().astimezone(self.bt.tvw_helpers.eastern_tz)
		self.exposure.update(when,snapshot)
		self.net_positions=snapshot['net_positions']
		return snapshot
	def record(self,when):
		snapshot=self.track_exposure(when);timestamp=when.isoformat();self.aggregate.realized_by_ticker={}
		unrealized_by_ticker={}
		for owner,book in self.books.items():
			if not self.enabled[owner]:continue
			realized=sum(book.realized_by_ticker.values());unrealized=exposure=0.
			for ticker,value in book.realized_by_ticker.items():self.aggregate.realized_by_ticker[ticker]=self.aggregate.realized_by_ticker.get(ticker,0.)+value
			for ticker,p in book.positions.items():
				price=self.prices.get(ticker,p.avg_price_per_share);value=(price-p.avg_price_per_share)*p.num_shares*(1 if p.side=='long' else -1)
				unrealized+=value;exposure+=price*p.num_shares;unrealized_by_ticker[ticker]=unrealized_by_ticker.get(ticker,0.)+value
			report=self.strategy_reports[owner];report.update(realized_pnl=realized,unrealized_pnl=unrealized,total_pnl=realized+unrealized)
			report['overall_pnl_history'].append(dict(time=timestamp,overall_total_pnl=realized+unrealized,gross_open_exposure=exposure))
		for direction,report in self.direction_reports.items():
			unrealized=exposure=0.
			for owner,book in self.books.items():
				if not self.enabled[owner]:continue
				for ticker,p in book.positions.items():
					if self.direction(owner,p.side)==direction:
						price=self.prices.get(ticker,p.avg_price_per_share);unrealized+=(price-p.avg_price_per_share)*p.num_shares*(1 if p.side=='long' else -1);exposure+=price*p.num_shares
			realized=self.realized_directions[direction];report.update(realized_pnl=realized,unrealized_pnl=unrealized,total_pnl=realized+unrealized)
			report['overall_pnl_history'].append(dict(time=timestamp,overall_total_pnl=realized+unrealized,gross_open_exposure=exposure))
		for ticker in set(self.prices)|set(self.aggregate.realized_by_ticker):
			r=self.aggregate.realized_by_ticker.get(ticker,0.);u=unrealized_by_ticker.get(ticker,0.)
			self.aggregate.ticker_pnl_history.setdefault(ticker,[]).append(dict(time=timestamp,ticker=ticker,realized_pnl=r,unrealized_pnl=u,total_pnl=r+u))
		self.aggregate.overall_pnl_history.append(dict(time=timestamp,overall_total_pnl=sum(self.aggregate.realized_by_ticker.values())+sum(unrealized_by_ticker.values()),gross_open_exposure=snapshot['gross_open_exposure'],virtual_ledger_open_exposure=snapshot['gross_open_exposure'],net_open_exposure=snapshot['net_open_exposure'],capital_open_exposure=snapshot['net_open_exposure'],net_long_open_exposure=snapshot['net_long_open_exposure'],net_short_open_exposure=snapshot['net_short_open_exposure']))
