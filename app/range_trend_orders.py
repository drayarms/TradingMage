"""DAY limit fills for independent books, using later completed minute bars."""
import math
import pandas as pd
from backtest_limit_orders import ExtendedLimitOrders

class RangeTrendLimitOrders(ExtendedLimitOrders):
	def submit(self,owner,event,side,qty=None,exit_order=False):
		book=self.p.books[owner];calendar=book.market_data.get('execution_sessions')
		if event.get('_limit_fill') or calendar is None or not calendar.extended(event['received_dt']):return False
		key=(owner,event['ticker'])
		if not exit_order and not self.p.prepare_entry(owner,event['ticker'],side,event):return True
		if key in self.pending:
			if exit_order and not self.pending[key]['exit_order']:self.finish(key,event['received_dt'],'cancelled_by_exit')
			else:return True
		try:
			quote=self.p.bt._get_simulated_quote(book,event);buy=(side=='long') if not exit_order else side=='short';limit=self.p.bt._get_execution_price(quote,'buy' if buy else 'sell')
		except ValueError:return True
		when,bounds=calendar.bounds(event['received_dt'])
		self.pending[key]=dict(owner=owner,event=dict(event),side=side,qty=qty,exit_order=exit_order,buy=buy,limit=limit,submitted=when,expires=bounds[3],position=book.positions.get(key[1]) if exit_order else None)
		self.events.append(dict(ticker=key[1],strategy_type=owner,time=when.isoformat(),status='submitted',action='exit' if exit_order else 'entry',order_type='limit',extended_hours=True,limit_price=limit))
		return True
	def process(self,when,batch):
		bars={x['payload']['ticker']:x['payload'] for x in batch if x['kind']=='market_bar'}
		for key,(original,position) in list(self.closed_exit_intents.items()):
			book=self.p.books[key[0]]
			if book.positions.get(key[1]) is not position:self.closed_exit_intents.pop(key,None);continue
			if book.market_data['execution_sessions'].allows(when):
				event=dict(original,received_dt=pd.Timestamp(when).to_pydatetime(),time=pd.Timestamp(when).isoformat())
				if self.p.bt._close_position(book,event):self.p.sync(key[0]);self.closed_exit_intents.pop(key,None)
				elif key in self.pending:self.closed_exit_intents.pop(key,None)
		volume_left={}
		orders=sorted(list(self.pending.items()),key=lambda item:(not item[1]['exit_order'],self.p.family(item[0][0])!=self.p.settings.entry_strategy_precedence))
		for key,order in orders:
			if key not in self.pending:continue
			owner,ticker=key;book=self.p.books[owner];calendar=book.market_data['execution_sessions'];position=book.positions.get(ticker)
			if pd.Timestamp(when)>=order['expires']:self.finish(key,when,'expired');continue
			if order['exit_order'] and position is not order['position']:self.finish(key,when,'cancelled_stale_position');continue
			if not order['exit_order'] and self.p.trend_entry_opposing_1h_trend(owner,ticker,order['side'],when):
				self.finish(key,when,'cancelled_trend_opposing_1h_trend');continue
			if not order['exit_order'] and owner.startswith('trend_'):
				_,row=self.p.context(ticker,when)
				if row is None or row.get(owner+'_reversal_id')!=order['event'].get('_trend_reversal_id'):
					self.finish(key,when,'cancelled_reversal_window_ended');continue
			if not order['exit_order'] and self.p.range_entry_opposing_trend(owner,ticker,order['side'],when):
				self.finish(key,when,'cancelled_range_opposing_trend');continue
			if not order['exit_order'] and self.p.range_entry_opposing_1h_trend(owner,ticker,order['side'],when):
				self.finish(key,when,'cancelled_range_opposing_1h_trend');continue
			bar=bars.get(ticker)
			if bar is None:continue
			start=pd.Timestamp(bar['source_bar_time'])
			if start<order['submitted'] or not calendar.same_session(start,when):continue
			if (bar['low']>order['limit'] if order['buy'] else bar['high']<order['limit']):continue
			event=dict(order['event'],received_dt=pd.Timestamp(when).to_pydatetime(),time=pd.Timestamp(when).isoformat(),_limit_fill=True,_execution_fill_price=order['limit'],_decision_price=float(bar['close']))
			if order['exit_order']:
				if not self.p.exit_allowed(owner,event):continue
				qty=position.num_shares
			else:
				qty=order['qty']
				if owner.startswith('trend_'):qty=self.p.trend_quantity(owner,ticker,order['limit'])
				else:qty=min(qty,max(0.,self.p.base*self.p.settings.max_exposure_multiple-self.p.range_exposure(ticker,order['limit'],order['side']))/order['limit'])
				if qty<=0:self.finish(key,when,'cancelled_exposure_cap');continue
			volume_key=(ticker,start.value,self.p.enabled[owner]);available=volume_left.setdefault(volume_key,self.volumes.get(ticker,{}).get(start.value,0.))
			replacement_qty=0.
			if not order['exit_order'] and self.p.family(owner)==self.p.settings.entry_strategy_precedence:
				replacement_qty=sum(pos.num_shares for other,b in self.p.books.items() if self.p.active(other) and self.p.family(other)!=self.p.family(owner) and self.p.enabled[other]==self.p.enabled[owner] and (pos:=b.positions.get(ticker)) is not None)
			if not math.isfinite(available) or qty+replacement_qty>available:continue
			if order['exit_order']:filled=self.p.bt._close_position(book,event)
			else:
				if not self.p.prepare_entry(owner,ticker,order['side'],event):
					if self.pending.get(key) is order:self.finish(key,when,'cancelled_entry_no_longer_valid')
					continue
				filled=self.p.bt._open_or_add_position(book,event,order['side'],qty)
				if filled and not owner.startswith('trend_'):
					index,_=self.p.context(ticker,when);self.p.last_range_entry[ticker,order['side']]=index
			if filled:
				volume_left[volume_key]=available-qty-replacement_qty
				book.trade_events[-1].update(entry_trigger=event.get('entry_trigger'),signal_time=order['event']['time'],exit_reason=event.get('exit_reason'),order_type='limit',limit_price=order['limit'])
				self.p.sync(owner);self.finish(key,when,'filled')
