"""Persisted, idempotent net-inventory reconciliation for one Alpaca account.

Call under the account lock used by RangeTrendLive. Virtual book targets and
broker fills are distinct records: unfilled orders never count as broker stock.
"""
from decimal import Decimal,ROUND_DOWN,ROUND_UP
import hashlib,json,math
import pandas as pd
from backtest_sessions import BacktestSessions

TERMINAL={'filled','canceled','cancelled','expired','rejected','replaced'}

def missing(exc):return getattr(exc,'status_code',None)==404 or getattr(getattr(exc,'response',None),'status_code',None)==404

class NetBroker:
	def __init__(self,api,redis,prefix,check_lock=lambda:None):self.api,self.r,self.prefix,self.check_lock=api,redis,prefix,check_lock
	def load(self,key):
		value=self.r.get(self.prefix+key);return json.loads(value) if value else {}
	def save(self,key,value):self.check_lock();self.r.set(self.prefix+key,json.dumps(value,allow_nan=False))
	def position(self,symbol):
		try:return Decimal(str(self.api.get_position(symbol).qty))
		except Exception as exc:
			if missing(exc):return Decimal(0)
			raise
	def lookup(self,client):
		try:return self.api.get_order_by_client_order_id(client)
		except Exception as exc:
			if missing(exc):return None
			raise
	def reconcile(self,symbol,target,now):
		now=pd.Timestamp(now);target=Decimal(str(target))
		if not target.is_finite():raise ValueError('Net target must be finite')
		calendar=BacktestSessions.load(self.api,now,now)
		if not calendar.allows(now):return dict(status='outside_execution_session')
		account=self.api.get_account()
		if getattr(account,'trading_blocked',False):return dict(status='account_trading_blocked')
		state=self.load('net:'+symbol);actual=self.position(symbol)
		if not state.get('owned') and actual!=0:return dict(status='unmanaged_existing_position',actual=str(actual))
		orders=list(self.api.list_orders(status='open',symbols=[symbol]))
		pending=state.get('pending')
		if any(getattr(o,'client_order_id',None)!=(pending or {}).get('client') for o in orders):return dict(status='unmanaged_open_orders')
		if pending:
			order=self.lookup(pending['client'])
			if order is not None:
				status=str(order.status)
				if status not in TERMINAL:
					if Decimal(pending['target'])!=target:
						self.check_lock();self.api.cancel_order(order.id)
						return dict(status='waiting_for_target_change_cancellation',order_id=str(order.id))
					return dict(status='order_pending',order_id=str(order.id),actual=str(actual),target=str(target))
				state.pop('pending',None);self.save('net:'+symbol,state);actual=self.position(symbol)
			elif Decimal(pending['target'])!=target:
				# The old request may still be in flight. Resolve its id before superseding.
				return dict(status='awaiting_uncertain_order_resolution')
		if actual==target:
			state.update(owned=True);self.save('net:'+symbol,state);return dict(status='at_target',actual=str(actual))
		asset=self.api.get_asset(symbol)
		if not getattr(asset,'tradable',False):return dict(status='asset_not_tradable')
		if target<0 and actual<=0 and not getattr(asset,'shortable',False):return dict(status='asset_not_shortable')
		extended=calendar.extended(now);fractional=bool(getattr(asset,'fractionable',False)) and target>=0 and actual>=0 and not extended
		step=Decimal('.000000001') if fractional else Decimal(1)
		# Round inventory intent toward zero; never manufacture fractional shorts.
		rounded=target.quantize(step,rounding=ROUND_DOWN);delta=rounded-actual
		if actual*rounded<0:delta=-actual # Flatten before opening the reverse broker side.
		if delta==0:return dict(status='at_rounded_target',actual=str(actual),target=str(target))
		if extended and delta%1:return dict(status='fractional_delta_waiting_for_core_session')
		quote=self.api.get_latest_quote(symbol);stamp=pd.Timestamp(quote.timestamp)
		ask,bid=Decimal(str(quote.ask_price)),Decimal(str(quote.bid_price))
		if stamp.tzinfo is None or not 0<=(now-stamp).total_seconds()<=90 or not (ask.is_finite() and bid.is_finite() and ask>=bid>0):return dict(status='invalid_or_stale_quote')
		buy=delta>0;raw=ask if buy else bid;tick=Decimal('.01') if raw>=1 else Decimal('.0001');limit=raw.quantize(tick,rounding=ROUND_UP if buy else ROUND_DOWN)
		pending=state.get('pending')
		if pending is None:
			seq=state.get('sequence',0)+1;client='rt-'+hashlib.sha256((self.prefix+symbol+str(seq)).encode()).hexdigest()[:40]
			pending=dict(client=client,target=str(target),qty=str(abs(delta)),side='buy' if buy else 'sell',limit=str(limit),extended=extended)
			state.update(sequence=seq,owned=True,pending=pending);self.save('net:'+symbol,state)
		# A submission timeout is recovered by looking up this same persisted client id.
		self.check_lock()
		order=self.api.submit_order(symbol=symbol,qty=pending['qty'],side=pending['side'],type='limit',time_in_force='day',limit_price=pending['limit'],extended_hours=pending['extended'],client_order_id=pending['client'])
		return dict(status='net_order_submitted',order_id=str(order.id),actual=str(actual),target=str(target))
