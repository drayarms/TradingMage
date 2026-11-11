"""Causal DAY limit-order approximation using subsequent completed 1m bars."""
import math
import pandas as pd

class ExtendedLimitOrders:
	def __init__(self,portfolio):
		self.p=portfolio
		self.pending={}
		self.events=[]
		self.closed_exit_intents={}
		self.volumes={t:{pd.Timestamp(k).value:float(v) for k,v in values.items()} for t,values in portfolio.books['cross'].market_data.get('volume_1m',{}).items()}

	def defer_closed_exit(self,owner,event):
		position=self.p.books[owner].positions.get(event['ticker'])
		if position is not None:self.closed_exit_intents[owner,event['ticker']]=(dict(event),position)


	def submit(self,owner,event,side,qty=None,exit_order=False):
		book=self.p.books[owner];calendar=book.market_data.get('execution_sessions')
		if event.get('_limit_fill') or calendar is None or not calendar.extended(event['received_dt']): return False
		ticker=event['ticker'];key=(owner,ticker)
		if not exit_order:
			reason=self.p.entry_zone_reason(side,ticker,event['received_dt'],owner=owner)
			if reason:
				existing=self.pending.get(key)
				if existing and not existing['exit_order']:self.finish(key,event['received_dt'],'cancelled_'+reason)
				self.events.append(dict(ticker=ticker,strategy_type=owner,time=pd.Timestamp(event['received_dt']).isoformat(),status='rejected_'+reason,action='entry'))
				return True
		if key in self.pending:
			if exit_order and not self.pending[key]['exit_order']: self.finish(key,event['received_dt'],'cancelled_by_exit')
			else: return True
		claim=self.p.entry_claims.get((ticker,pd.Timestamp(event['received_dt']).value))
		if not exit_order and claim and claim[0]!=owner and claim[1]!=side: return True
		try:
			quote=self.p.bt._get_simulated_quote(book,event)
			buy=(side=='long') if not exit_order else (side=='short')
			limit=self.p.bt._get_execution_price(quote,'buy' if buy else 'sell')
		except ValueError: return True
		if not exit_order:
			for other_key,other in list(self.pending.items()):
				if other_key[0]!=owner and other_key[1]==ticker and not other['exit_order'] and other['side']!=side:
					self.finish(other_key,event['received_dt'],'cancelled_by_opposite_entry')
		when,bounds=calendar.bounds(event['received_dt'])
		self.pending[key]=dict(owner=owner,event=dict(event),side=side,qty=qty,exit_order=exit_order,buy=buy,limit=limit,submitted=when,expires=bounds[3],position=book.positions.get(ticker) if exit_order else None)
		self.events.append(dict(ticker=ticker,strategy_type=owner,time=when.isoformat(),status='submitted',order_type='limit',time_in_force='day',extended_hours=True,limit_price=limit,action='exit' if exit_order else 'entry'))
		if not exit_order: self.p.claim_entry(owner,ticker,side,event)
		return True

	def process(self,when,batch):
		bars={item['payload']['ticker']:item['payload'] for item in batch if item['kind']=='market_bar'}
		for key,(original,position) in list(self.closed_exit_intents.items()):
			book=self.p.books[key[0]]
			if book.positions.get(key[1]) is not position:
				self.closed_exit_intents.pop(key,None);continue
			if not book.market_data['execution_sessions'].allows(when): continue
			event=dict(original,received_dt=pd.Timestamp(when).to_pydatetime(),time=pd.Timestamp(when).isoformat())
			closed=self.p.bt._close_position(book,event)
			if closed:
				book.trade_events[-1]['exit_reason']=event.get('exit_reason','deferred_exit');self.p.sync(key[0])
			if closed or key in self.pending: self.closed_exit_intents.pop(key,None)
		remaining_volume={}
		for key,order in sorted(list(self.pending.items()),key=lambda item:((('cross' if item[0][0]=='cross' else 'range')!=self.p.settings.entry_strategy_precedence),item[0][0]=='range')):
			if key not in self.pending: continue
			if order['exit_order']:
				book=self.p.books[key[0]]
				if book.positions.get(key[1]) is not order['position']:
					self.finish(key,when,'cancelled_stale_position');continue
				check=dict(order['event'],received_dt=pd.Timestamp(when).to_pydatetime())
				if not self.p.exit_allowed(key[0],check): continue
			if pd.Timestamp(when)>=order['expires']:
				self.finish(key,when,'expired');continue
			if not order['exit_order']:
				reason=self.p.entry_zone_reason(order['side'],key[1],when,owner=key[0])
				if reason:self.finish(key,when,'cancelled_'+reason);continue
			ticker=key[1];bar=bars.get(ticker)
			if bar is None: continue
			start=pd.Timestamp(bar['source_bar_time'])
			calendar=self.p.books[key[0]].market_data['execution_sessions']
			if start<order['submitted'] or not calendar.allows(when) or not calendar.same_session(start,when): continue
			volume_key=(ticker,start.value)
			volume=remaining_volume.setdefault(volume_key,self.volumes.get(ticker,{}).get(start.value,0.))
			if not math.isfinite(volume) or volume<=0: continue
			if (bar['low']>order['limit'] if order['buy'] else bar['high']<order['limit']): continue
			owner=key[0];book=self.p.books[owner]
			event=dict(order['event'],received_dt=pd.Timestamp(when).to_pydatetime(),time=pd.Timestamp(when).isoformat(),_limit_fill=True,_execution_fill_price=order['limit'])
			position=book.positions.get(ticker)
			if not order['exit_order'] and owner=='post_stretch_range':
				from range_strategy import post_stretch_entry_side
				_,row=self.p.context(ticker,when)
				if row is None or post_stretch_entry_side(order['limit'],row)!=order['side']:
					self.finish(key,when,'cancelled_post_stretch_entry_no_longer_valid');continue
			entry_qty=None
			if not order['exit_order'] and owner=='cross':
				capacity=max(0.,self.p.base*self.p.settings.max_exposure_multiple-(self.p.range_exposure(ticker,order['limit'],order['side']) if owner!='cross' else position.num_shares*order['limit'] if position else 0.))
				entry_qty=min(order['qty'],capacity/order['limit'])
				if owner=='cross':
					decision=self.p.cross_entry_decision(ticker,order['side'],when,order['limit'])
					if not decision['eligible']:
						self.finish(key,when,'cancelled_'+decision['reason']);continue
					entry_qty=min(entry_qty,decision['quantity'])
				if entry_qty<=0: self.finish(key,when,'cancelled_exposure_cap');continue
			requested=position.num_shares if order['exit_order'] and position is not None else entry_qty if owner=='cross' else order['qty']
			if not order['exit_order']:
				requested+=sum(pos.num_shares for other,other_book in self.p.books.items() if other!=owner and self.p.active(other) and (pos:=other_book.positions.get(ticker)) is not None and pos.side!=order['side'])
			if requested is not None and requested>volume: continue
			if order['exit_order']:
				if position is None or position.side!=order['side']: self.finish(key,when,'cancelled');continue
				filled=self.p.bt._close_position(book,event)
				if filled:
					book.last_exit_time_by_ticker[ticker]=event['received_dt']
					book.trade_events[-1]['exit_reason']=event.get('exit_reason','limit_exit')
			else:
				if owner=='cross': qty=entry_qty
				else:
					capacity=max(0.,self.p.base*self.p.settings.max_exposure_multiple-(self.p.range_exposure(ticker,order['limit'],order['side']) if owner!='cross' else position.num_shares*order['limit'] if position else 0.))
					qty=min(order['qty'],capacity/order['limit'])
					if qty<=0: self.finish(key,when,'cancelled_exposure_cap');continue
				if not self.p.prepare_entry(owner,ticker,order['side'],event): continue
				filled=self.p.bt._open_or_add_position(book,event,order['side'],qty)
				if filled:
					if owner=='cross' and position is None and self.p.settings.exit_on_confirmatory_candlestick:
						if not hasattr(book,'confirmatory_pairs'): book.confirmatory_pairs={}
						book.confirmatory_pairs[ticker]=dict(position=book.positions[ticker],opened_at=event['received_dt'])
					self.p.claim_entry(owner,ticker,order['side'],event)
					if owner=='cross': self.p.record_cross_fill(ticker,order['side'],event)
					book.trade_events[-1].update(entry_trigger=event.get('entry_trigger','range_band_proximity' if owner=='range' else self.p.settings.entry_trigger),order_type='limit',limit_price=order['limit'],order_submitted_at=order['submitted'].isoformat())
					book.trade_events[-1]['signal_time']=order['event'].get('time')
					for name in ('range_entry_fraction','atr_price_ratio','range_context_completed_at','turning_point_evaluated_at'):
						if name in event:book.trade_events[-1][name]=event[name]
					if owner!='cross':
						index,_=self.p.context(ticker,when);self.p.last_range_entry[ticker,order['side']]=index
			if filled:
				remaining_volume[volume_key]=volume-requested
				self.p.track_exposure(when)
				self.p.sync(owner);self.finish(key,when,'filled')

	def finish(self,key,when,status):
		order=self.pending.pop(key,None)
		if order:self.events.append(dict(ticker=key[1],strategy_type=key[0],time=pd.Timestamp(when).isoformat(),status=status,limit_price=order['limit']))
