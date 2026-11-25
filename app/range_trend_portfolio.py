"""Independent virtual inventory books, with family-level precedence."""
import pandas as pd
from strategy_portfolio import StrategyPortfolio
from range_trend_settings import STRATEGY_NAME
from trend_strategy import entry_reason,exit_reason,observe_band_visit
import math
from trend_entry_guard import PotentialUnprofitableTrendGuard
from range_outside_zone import annotate_outside_range_candles,outside_range_exit
from capital_exposure_report import NetAndLedgerExposureTracker,position_exposures

FORCED_TREND_EXIT_REASONS=frozenset({'range_opposing_trend','range_opposing_1h_trend','trend_opposing_1h_trend'})

class RangeTrendPortfolio(StrategyPortfolio):
	def __init__(self,bt,settings,state,frames,base):
		from backtester import SimState
		super().__init__(bt,settings,state,frames,base)
		self.rows={t:annotate_outside_range_candles(f).to_dict('records') for t,f in frames.items()}
		self.enabled['cross']=False
		for side in ('long','short'):
			owner='trend_'+side;book=SimState();book.market_data=state.market_data;book.last_price_by_ticker=self.prices;book.execution_price_index=state.execution_price_index
			self.books[owner]=book;self.enabled[owner]=settings.trend_strategy_enabled;self.cursors[owner]=0
			book.exit_allowed=lambda event:True
			book.on_position_closed=lambda ticker,when,owner=owner:self.position_closed(owner,ticker,when)
			book.on_entry_filled=lambda ticker,side,when,owner=owner:self.trend_filled(owner,ticker,when)
			book.defer_exit=lambda event,side,owner=owner:self.limit_orders.submit(owner,event,side,exit_order=True)
			book.defer_closed_exit=lambda event,owner=owner:self.limit_orders.defer_closed_exit(owner,event)
		self.hourly_trend_context={};self.hourly_trend_indexes={}
		self.last_trend_reversal={};self.trend_band_visits={}
		self.trend_entry_guard=PotentialUnprofitableTrendGuard();self.trend_entry_guard_events=[]
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
	def set_hourly_trends(self,runs,options=None):
		from hourly_trend_context import build_hourly_trends
		self.hourly_trend_context=build_hourly_trends(getattr(runs,'raw_frames',{}),self.settings,options)
		self.hourly_trend_indexes={t:(pd.DatetimeIndex(f.completed_at),f.strong_long_trend.to_numpy(dtype=bool),f.strong_short_trend.to_numpy(dtype=bool)) for t,f in self.hourly_trend_context.items() if not f.empty}
	def range_entry_opposing_1h_trend(self,owner,ticker,side,when):
		if owner not in {'range','post_stretch_range'}:return False
		flag='block_range_stretch_entries_on_opposing_1h_trend_filter_enabled' if owner=='post_stretch_range' else 'block_range_entries_on_opposing_1h_trend_filter_enabled'
		if not getattr(self.settings,flag):return False
		return self.opposing_1h_trend(ticker,side,when)
	def trend_entry_opposing_1h_trend(self,owner,ticker,side,when):
		return owner in {'trend_long','trend_short'} and self.settings.block_trend_entries_on_opposing_1h_trend_filter_enabled and self.opposing_1h_trend(ticker,side,when)
	def opposing_1h_trend(self,ticker,side,when):
		cached=self.hourly_trend_indexes.get(ticker)
		if cached is not None:
			times,longs,shorts=cached;i=times.searchsorted(pd.Timestamp(when),side='right')-1
			return i>=0 and bool((shorts if side=='long' else longs)[i])
		frame=self.hourly_trend_context.get(ticker)
		if frame is None or frame.empty:return False
		i=pd.DatetimeIndex(frame.completed_at).searchsorted(pd.Timestamp(when),side='right')-1
		return i>=0 and bool(frame.iloc[i]['strong_'+('short' if side=='long' else 'long')+'_trend'])
	def range_opposing_trend(self,owner,ticker,side,when):
		if owner not in {'range','post_stretch_range'}:return False
		_,row=self.context(ticker,when)
		return row is not None and bool(row.get('strong_'+('short' if side=='long' else 'long')+'_trend',False))
	def range_entry_opposing_trend(self,owner,ticker,side,when):
		flag='block_range_stretch_entries_on_opposing_trend_filter_enabled' if owner=='post_stretch_range' else 'block_range_entries_on_opposing_trend_filter_enabled'
		return getattr(self.settings,flag) and self.range_opposing_trend(owner,ticker,side,when)
	def entry_zone_reason(self,side,ticker,when,owner='range'):
		if owner.startswith('trend_'):return 'trend_opposing_1h_trend' if self.trend_entry_opposing_1h_trend(owner,ticker,side,when) else None
		if self.range_entry_opposing_trend(owner,ticker,side,when):return 'range_opposing_trend'
		if self.range_entry_opposing_1h_trend(owner,ticker,side,when):return 'range_opposing_1h_trend'
		return super().entry_zone_reason(side,ticker,when,owner)
	def exit_allowed(self,owner,event):
		if event.get('exit_reason') in FORCED_TREND_EXIT_REASONS:return True
		key=(owner,event['ticker']);pending=self.limit_orders.pending.get(key);closed=self.limit_orders.closed_exit_intents.get(key)
		# Preserve a forced exit's reason and intent until it can execute.
		if (pending and pending['exit_order'] and pending['event'].get('exit_reason') in FORCED_TREND_EXIT_REASONS) or (closed and closed[0].get('exit_reason') in FORCED_TREND_EXIT_REASONS):return False
		if owner.startswith('trend_') or event.get('exit_reason','').startswith('replaced_by_qualifying_'):return True
		return super().exit_allowed(owner,event)
	def process_opposing_1h_trend_exits(self,ticker,when):
		flags={'range':'force_range_exits_on_opposing_1h_trend_filter_enabled','post_stretch_range':'force_range_stretch_exits_on_opposing_1h_trend_filter_enabled','trend_long':'force_trend_exits_on_opposing_1h_trend_filter_enabled','trend_short':'force_trend_exits_on_opposing_1h_trend_filter_enabled'}
		for owner,flag in flags.items():
			book=self.books[owner];key=(owner,ticker);position=book.positions.get(ticker)
			if not self.active(owner) or position is None or not getattr(self.settings,flag) or not self.opposing_1h_trend(ticker,position.side,when):continue
			self.blocked_exit_intents.pop(key,None)
			pending=self.limit_orders.pending.get(key);closed=self.limit_orders.closed_exit_intents.get(key)
			if pending and pending['exit_order'] and pending['event'].get('exit_reason') in FORCED_TREND_EXIT_REASONS:continue
			if closed and closed[0].get('exit_reason') in FORCED_TREND_EXIT_REASONS:continue
			reason='trend_opposing_1h_trend' if owner.startswith('trend_') else 'range_opposing_1h_trend'
			if pending:self.limit_orders.finish(key,when,'superseded_by_'+reason)
			self.limit_orders.closed_exit_intents.pop(key,None)
			event=dict(self.range_event(ticker,when),exit_reason=reason)
			if self.bt._close_position(book,event):
				book.trade_events[-1]['exit_reason']=reason;self.sync(owner)
	def process_range_opposing_trend_exits(self,ticker,when):
		for owner in ('range','post_stretch_range'):
			if not self.active(owner):continue
			book=self.books[owner];key=(owner,ticker);position=book.positions.get(ticker)
			pending=self.limit_orders.pending.get(key)
			if pending and not pending['exit_order'] and self.range_entry_opposing_trend(owner,ticker,pending['side'],when):
				self.limit_orders.finish(key,when,'cancelled_range_opposing_trend');pending=None
			if pending and not pending['exit_order'] and self.range_entry_opposing_1h_trend(owner,ticker,pending['side'],when):
				self.limit_orders.finish(key,when,'cancelled_range_opposing_1h_trend');pending=None
			flag='force_range_stretch_exits_on_opposing_trend_filter_enabled' if owner=='post_stretch_range' else 'force_range_exits_on_opposing_trend_filter_enabled'
			if position is None or not getattr(self.settings,flag) or not self.range_opposing_trend(owner,ticker,position.side,when):continue
			self.blocked_exit_intents.pop(key,None)
			if pending and pending['exit_order']:
				if pending['event'].get('exit_reason')=='range_opposing_trend':continue
				self.limit_orders.finish(key,when,'superseded_by_range_opposing_trend')
			self.limit_orders.closed_exit_intents.pop(key,None)
			event=dict(self.range_event(ticker,when),exit_reason='range_opposing_trend')
			if self.bt._close_position(book,event):
				book.trade_events[-1]['exit_reason']='range_opposing_trend';self.sync(owner)
	def process_outside_range_exits(self,ticker,when):
		if not self.settings.exit_range_position_outside_range_zone:return
		_,row=self.context(ticker,when)
		for owner in ('range','post_stretch_range'):
			book=self.books[owner];position=book.positions.get(ticker)
			if not self.active(owner) or position is None or not outside_range_exit(position.side,row,self.settings.exit_range_position_outside_range_zone_candles):continue
			event=dict(self.range_event(ticker,when),exit_reason='range_position_outside_range_zone')
			if self.bt._close_position(book,event):
				book.trade_events[-1]['exit_reason']='range_position_outside_range_zone';self.sync(owner)
	def process_heikin_ashi_exits(self,ticker,when):
		self.process_outside_range_exits(ticker,when)
		# Legacy 1h filters apply only to the two range books.
		from heikin_ashi_filter import EXIT_REASON
		from golden_cross_dip_recovery import HEIKIN_ASHI_FILTER_FLAGS
		for owner in ('range','post_stretch_range'):
			book=self.books[owner];position=book.positions.get(ticker)
			if not self.active(owner) or position is None or not getattr(self.settings,HEIKIN_ASHI_FILTER_FLAGS[owner]['exit']):continue
			if self.heikin_ashi_runs.blocks(ticker,position.side,when):
				if self.bt._close_position(book,dict(self.range_event(ticker,when),exit_reason=EXIT_REASON)):self.sync(owner)
	def process_zone_exits(self,ticker,when,record_zone=False):
		self.process_range_opposing_trend_exits(ticker,when)
		self.process_opposing_1h_trend_exits(ticker,when)
		# Transition exits remain range-only. Trend exits use their own policy.
		trend_positions={o:b.positions for o,b in self.books.items() if o.startswith('trend_')}
		try:
			for o in trend_positions:self.books[o].positions={}
			super().process_zone_exits(ticker,when,record_zone)
		finally:
			for o,positions in trend_positions.items():self.books[o].positions=positions
	def entry_precedence_available(self,owner,ticker):
		if self.settings.entry_strategy_precedence in {'none',self.family(owner)}:return True
		return not any(ticker in b.positions or (o,ticker) in self.limit_orders.pending and not self.limit_orders.pending[o,ticker]['exit_order'] for o,b in self.books.items() if self.active(o) and self.family(o)!=self.family(owner) and self.enabled[o]==self.enabled[owner])
	def apply_trend_entry_guard(self,owner,ticker,event,row,reversal):
		price=event.get('_execution_fill_price',event.get('price'))
		result=self.trend_entry_guard.evaluate(owner,ticker,row,price,reversal,self.settings)
		if result['new_attempt']:
			state=result['state'];self.trend_entry_guard_events.append(dict(time=pd.Timestamp(event['received_dt']).isoformat(),ticker=ticker,strategy_direction=owner,cycle_id=state['cycle_id'],reversal_id=reversal,entry_price=price,reference_price=state['reference_price'],atr=row.get('atr'),atr_multiple=self.settings.potential_unprofitable_trend_entry_atr_multiple,attempt_count=state['attempt_count'],blocked=result['block'],liquidation_requested=result['liquidate']))
		if result['liquidate']:
			book=self.books[owner]
			if ticker in book.positions:
				# An entry limit is not an exit limit; obtain an independent exit quote.
				clean={k:v for k,v in event.items() if k not in {'_limit_fill','_execution_fill_price'}}
				if self.bt._close_position(book,dict(clean,exit_reason='potential_unprofitable_trend_entry')):
					book.trade_events[-1]['exit_reason']='potential_unprofitable_trend_entry';self.sync(owner)
		return not result['block']
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
		if self.trend_entry_opposing_1h_trend(owner,ticker,side,event['received_dt']):return False
		if owner.startswith('trend_'):
			_,row=self.context(ticker,event['received_dt']);price=event.get('_decision_price',event.get('_execution_fill_price',event.get('price')))
			if self.trend_exit_reason(owner,ticker,price,row):return False
			decision=entry_reason(side,price,row,own is not None,self.last_trend_reversal.get((owner,ticker)),cooldown_candles=self.settings.trend_entry_opposing_universal_zone_cooldown_candles)
			if not decision:return False
			# A pending fill must still belong to the same completed reversal candle.
			if event.get('_trend_reversal_id') and decision[1]!=event['_trend_reversal_id']:return False
			if not self.entry_precedence_available(owner,ticker):return False
			if not self.apply_trend_entry_guard(owner,ticker,event,row,decision[1]):return False
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
			trades=self.books[owner].trade_events
			if trades:self.trend_entry_guard.filled(owner,ticker,row,float(trades[-1]['price']))
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
			self.trend_entry_guard.state(owner,ticker,row)
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
