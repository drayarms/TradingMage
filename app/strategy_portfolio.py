"""Separate strategy-owned books with one shared per-ticker direction guard."""
import copy
import numpy as np
import pandas as pd
from golden_cross_dip_recovery import STRATEGY_NAME
from range_strategy import range_entry_side, range_exit_reason, range_entry_fraction, post_stretch_entry_side
from open_exposure_report import OpenExposureTracker
from backtest_limit_orders import ExtendedLimitOrders

class StrategyPortfolio:
	def __init__(self,bt,settings,cross_state,frames,base):
		from backtester import SimState
		self.bt,self.settings,self.base=bt,settings,base
		self.books={'cross':cross_state,'range':SimState(),'post_stretch_range':SimState()}
		self.enabled={'cross':settings.cross_strategy_enabled,'range':settings.range_strategy_enabled,'post_stretch_range':settings.range_strategy_enabled}
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
		self.last_cross_entry={}
		self.cross_snapshot_cache={}
		self.blocked_exit_intents={}
		self.rejections=set()
		self.entry_claims={}
		self.rows={t:f.to_dict('records') for t,f in frames.items()}
		self.times={t:pd.DatetimeIndex(f.completed_at).asi8 for t,f in frames.items() if not f.empty}
		self.strategy_reports={owner:dict(enabled=enabled,overall_pnl_history=[]) for owner,enabled in self.enabled.items()}
		self.direction_reports={name:dict(overall_pnl_history=[]) for name in ('golden_cross','death_cross','range_long','range_short','post_stretch_range_long','post_stretch_range_short')}
		self.realized_directions=dict.fromkeys(self.direction_reports,0.)
		self.limit_orders=ExtendedLimitOrders(self)
		for owner,book in self.books.items():
			book.exit_allowed=lambda event,owner=owner:self.exit_allowed(owner,event)
			book.on_position_closed=lambda ticker,when,owner=owner:self.position_closed(owner,ticker,when)
			book.defer_exit=lambda event,side,owner=owner:self.limit_orders.submit(owner,event,side,exit_order=True)
			book.defer_closed_exit=lambda event,owner=owner:self.limit_orders.defer_closed_exit(owner,event)

	def exit_allowed(self,owner,event):
		from cross_entry_policy import exit_gradient_blocked
		key=(owner,event['ticker']);book=self.books[owner]
		position=book.positions.get(event['ticker'])
		if position is None: return True
		_,row=self.context(event['ticker'],event['received_dt'])
		if row is None or not exit_gradient_blocked(position.side,row): return True
		existing=self.blocked_exit_intents.get(key)
		if existing is None or existing[1] is not position:
			clean={k:v for k,v in event.items() if k not in {'_limit_fill','_execution_fill_price'}}
			self.blocked_exit_intents[key]=(clean,position)
			self.limit_orders.events.append(dict(ticker=key[1],strategy_type=owner,time=pd.Timestamp(event['received_dt']).isoformat(),status='blocked_by_three_gradients',action='exit',exit_reason=event.get('exit_reason')))
		order=self.limit_orders.pending.get(key)
		if order and order['exit_order']: self.limit_orders.finish(key,event['received_dt'],'cancelled_by_three_gradient_veto')
		self.limit_orders.closed_exit_intents.pop(key,None)
		return False

	def position_closed(self,owner,ticker,when):
		key=(owner,ticker)
		self.books[owner].last_exit_time_by_ticker[ticker]=when
		self.blocked_exit_intents.pop(key,None)
		self.limit_orders.closed_exit_intents.pop(key,None)
		order=self.limit_orders.pending.get(key)
		if order and not order['exit_order']: self.limit_orders.finish(key,when,'cancelled_by_position_close')

	def process_exit_intents(self,when):
		"""Retry remembered exits before fresh entries, at current execution prices."""
		for key,(original,position) in list(self.blocked_exit_intents.items()):
			book=self.books[key[0]]
			if book.positions.get(key[1]) is not position:
				self.blocked_exit_intents.pop(key,None);continue
			event=dict(original,received_dt=pd.Timestamp(when).to_pydatetime(),time=pd.Timestamp(when).isoformat(),signal_time=original.get('signal_time',original.get('time')))
			if not self.exit_allowed(key[0],event): continue
			order=self.limit_orders.pending.get(key)
			if order and order['exit_order']: continue
			if self.bt._close_position(book,event):
				book.trade_events[-1].update(exit_reason=original.get('exit_reason','deferred_exit'),signal_time=event['signal_time'])
				self.sync(key[0])

	def active(self,owner):
		return self.enabled[owner] or self.settings.disabled_strategy_virtual_positions_enabled

	def allows(self,owner,ticker,side):
		if not self.active(owner): return False
		return not any(self.active(other) and ticker in book.positions and book.positions[ticker].side!=side for other,book in self.books.items())

	def prepare_entry(self,owner,ticker,side,event):
		"""Called only after sizing and all strategy entry requirements pass."""
		if not self.active(owner): return False
		if owner!='cross':
			index,_=self.context(ticker,event['received_dt']);last=self.last_range_entry.get((ticker,side))
			if last is not None and (index is None or index-last<2): return False
		if owner=='post_stretch_range':
			_,row=self.context(ticker,event['received_dt'])
			if row is None or post_stretch_entry_side(event.get('_execution_fill_price',event.get('price',float('nan'))),row)!=side:return False
		if owner=='cross' and self.cross_entry_spacing_reason(ticker,side,event['received_dt']): return False
		if not self.bt._execution_session_allowed(self.books[owner],event['received_dt']): return False
		if event['received_dt'].date().isoformat() in self.books[owner].market_close_liquidation_dates: return False
		own=self.books[owner].positions.get(ticker)
		if own and own.side!=side: return False
		when=pd.Timestamp(event['received_dt']).value
		claim=self.entry_claims.get((ticker,when))
		if claim and claim[0]!=owner and claim[1]!=side: return False
		if not event.get('_limit_fill'):
			self.limit_orders.finish((owner,ticker),event['received_dt'],'cancelled_by_new_entry')
		for key,order in list(self.limit_orders.pending.items()):
			if key[0]!=owner and key[1]==ticker and not order['exit_order'] and order['side']!=side:
				self.limit_orders.finish(key,event['received_dt'],'cancelled_by_opposite_entry')
		for other,book in self.books.items():
			position=book.positions.get(ticker)
			if other==owner or not self.active(other) or not position or position.side==side: continue
			self.track_exposure(event['received_dt'])
			reason='replaced_by_qualifying_'+owner+'_entry'
			if not self.bt._close_position(book,dict(event,exit_reason=reason)): return False
			book.trade_events[-1]['exit_reason']=reason
			book.last_exit_time_by_ticker[ticker]=event['received_dt']
			self.sync(other)
			self.limit_orders.finish((other,ticker),event['received_dt'],'cancelled_by_replacement')
		return True

	def claim_entry(self,owner,ticker,side,event):
		self.entry_claims[ticker,pd.Timestamp(event['received_dt']).value]=(owner,side)

	def cross_entry_spacing_reason(self,ticker,side,when):
		last=self.last_cross_entry.get((ticker,side))
		if last is None: return None
		index,_=self.context(ticker,when)
		return 'cross_entry_spacing' if index is None or index-last<2 else None

	def record_cross_fill(self,ticker,side,event):
		index,_=self.context(ticker,event['received_dt'])
		if index is not None: self.last_cross_entry[ticker,side]=index

	def cross_entry_snapshot(self,ticker,when):
		index,row=self.context(ticker,when)
		if row is None: return {}
		key=(ticker,index)
		if key not in self.cross_snapshot_cache:
			clean={}
			for name,value in row.items():
				if pd.isna(value): value=None
				elif isinstance(value,pd.Timestamp): value=value.isoformat()
				elif isinstance(value,np.generic): value=value.item()
				clean[name]=value
			self.cross_snapshot_cache[key]=clean
		return self.cross_snapshot_cache[key]

	def cross_entry_decision(self,ticker,side,when,price):
		from cross_entry_policy import evaluate_cross_entry
		if self.settings.strategy_mode not in {'both','golden_cross' if side=='long' else 'death_cross'}:
			return dict(eligible=False,reason='strategy_mode_disallows_side',quantity=0.)
		spacing=self.cross_entry_spacing_reason(ticker,side,when)
		if spacing: return dict(eligible=False,reason=spacing,quantity=0.)
		position=self.books['cross'].positions.get(ticker)
		if position and position.side!=side:
			return dict(eligible=False,reason='opposite_cross_position_open',quantity=0.)
		return evaluate_cross_entry(side,price,self.base,self.cross_entry_snapshot(ticker,when),
			position.num_shares*price if position else 0.,0.,self.settings)

	def continuous_cross_event(self,ticker,when):
		if not self.active('cross'): return None
		event=self.range_event(ticker,when)
		if not self.bt._execution_session_allowed(self.books['cross'],event['received_dt']): return None
		row=self.cross_entry_snapshot(ticker,when)
		if not row or row.get('fast_moving_average') is None or row.get('slow_moving_average') is None: return None
		position=self.books['cross'].positions.get(ticker)
		aligned='long' if row.get('fast_moving_average',0)>row.get('slow_moving_average',0) else 'short'
		sides=[position.side] if position else [aligned,'short' if aligned=='long' else 'long']
		try: quote=self.bt._get_simulated_quote(self.books['cross'],event)
		except ValueError: return None
		for side in sides:
			price=self.bt._get_execution_price(quote,'buy' if side=='long' else 'sell')
			decision=self.cross_entry_decision(ticker,side,when,price)
			if decision['eligible']:
				return dict(event,signal='_cross_entry',_continuous_entry_side=side,_entry_snapshot=row)
			index,_=self.context(ticker,when)
			key=('cross',ticker,side,index,decision['reason'])
			if key not in self.rejections:
				self.diagnostics.append(dict(ticker=ticker,time=event['time'],strategy_type='cross',
					is_virtual=not self.enabled['cross'],side=side,reason=decision['reason']))
				self.rejections.add(key)
		return None

	def direction(self,owner,side):
		return ('golden_cross' if side=='long' else 'death_cross') if owner=='cross' else owner+'_'+side

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

	def range_exposure(self,ticker,price,side=None):
		return sum(book.positions[ticker].num_shares*price for owner,book in self.books.items() if owner!='cross' and self.active(owner) and ticker in book.positions and (side is None or book.positions[ticker].side==side))

	def process_range(self,ticker,when,entries=False):
		if entries and self.active('post_stretch_range'):
			_,row=self.context(ticker,when)
			if row is not None:
				try:
					quote=self.bt._get_simulated_quote(self.books['post_stretch_range'],self.range_event(ticker,when))
					side=post_stretch_entry_side(float(quote['market']),row)
					price=self.bt._get_execution_price(quote,'buy' if side=='long' else 'sell') if side else None
				except ValueError:side=None;price=None
				if side and post_stretch_entry_side(price,row)==side:
					self.process_range_book('post_stretch_range',ticker,when,True)
					return
		if entries:self.process_range_book('range',ticker,when,True)
		else:
			for owner in ('post_stretch_range','range'):self.process_range_book(owner,ticker,when,False)

	def process_range_book(self,owner,ticker,when,entries=False):
		if not self.active(owner): return
		index,row=self.context(ticker,when)
		if row is None: return
		if entries and not row.get('ranging_market',False): return
		book=self.books[owner]; position=book.positions.get(ticker)
		if not entries and position is None: return
		if entries and book.last_exit_time_by_ticker.get(ticker)==when: return
		event=self.range_event(ticker,when)
		try: quote=self.bt._get_simulated_quote(book,event)
		except ValueError: return
		market=float(quote['market'])
		if not entries:
			cross='golden_cross' if position.side=='long' else 'death_cross'
			native_cross=bool(row.get(cross,False)) and pd.Timestamp(row['completed_at'])==pd.Timestamp(when) and (position.entry_time is None or pd.Timestamp(position.entry_time)<pd.Timestamp(when))
			reason='range_4h_'+cross if native_cross else range_exit_reason(position.side,market,self.rows[ticker],index,projection_candles=3 if owner=='post_stretch_range' else 1)
			if reason and owner=='post_stretch_range':reason='post_stretch_'+reason
			if reason:
				if self.bt._close_position(book,dict(event,exit_reason=reason)):
					book.last_exit_time_by_ticker[ticker]=when
					book.trade_events[-1]['exit_reason']=reason
				self.sync(owner)
			return
		side=post_stretch_entry_side(market,row) if owner=='post_stretch_range' else range_entry_side(market,row,self.settings.range_min_bollinger_gap_to_peak_since_turning_point_ratio)
		if side is None: return
		last=self.last_range_entry.get((ticker,side))
		if last is not None and index-last<2: return
		reason=None

		price=self.bt._get_execution_price(quote,'buy' if side=='long' else 'sell')
		if owner=='post_stretch_range' and post_stretch_entry_side(price,row)!=side:return
		event['price']=price
		trigger='post_stretch_range_fast_average' if owner=='post_stretch_range' else 'range_band_proximity'
		fraction=range_entry_fraction(price,float(row['atr']),self.settings)
		exposure=self.range_exposure(ticker,price,side)
		amount=min(self.base*fraction,max(0.,self.base*self.settings.max_exposure_multiple-exposure))
		if amount<=0: reason=reason or 'exposure_cap_reached'
		if reason:
			key=(ticker,side,index,reason)
			if key not in self.rejections:
				self.diagnostics.append(dict(ticker=ticker,time=when.isoformat(),strategy_type=owner,is_virtual=not self.enabled[owner],reason=reason))
				self.rejections.add(key)
			return
		event.update(entry_trigger=trigger,range_entry_fraction=fraction,atr_price_ratio=float(row['atr'])/price,range_context_completed_at=pd.Timestamp(row['completed_at']).isoformat())
		if self.limit_orders.submit(owner,event,side,amount/price): return
		if not self.prepare_entry(owner,ticker,side,event): return
		previous=len(book.trade_events)
		self.bt._open_or_add_position(book,event,side,amount/price)
		trades=book.trade_events[previous:]
		if any(e.get('event_type') in {'open','add'} for e in trades):
			self.claim_entry(owner,ticker,side,event)
			self.last_range_entry[ticker,side]=index
			for trade in trades:
				trade.update(entry_trigger=trigger,range_entry_fraction=fraction,atr_price_ratio=float(row['atr'])/price,range_context_completed_at=pd.Timestamp(row['completed_at']).isoformat())
		self.sync(owner)

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
