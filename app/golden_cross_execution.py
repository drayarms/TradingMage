"""Account-serialized broker adapter with deterministic order IDs and exit reconciliation."""
from decimal import Decimal, ROUND_DOWN, ROUND_UP
import hashlib
import json
import logging
import math
import threading
import pandas as pd
from golden_cross_dip_recovery import evaluate_entry, evaluate_signal_exit
from golden_cross_simulation import signal_action, event_identity

logger = logging.getLogger('tv-webhook')

TERMINAL = {'filled','canceled','expired','rejected','replaced'}

class GoldenCrossExecutor:
	def __init__(self, api, redis_client, history, settings, base_amount, enabled=False, engine=evaluate_entry):
		self.api,self.r,self.history,self.settings = api,redis_client,history,settings
		self.base_amount=float(base_amount);self.enabled=enabled;self.engine=engine
		if not math.isfinite(self.base_amount) or self.base_amount<=0:
			raise ValueError('base_amount must be finite and positive')
		self.account_id=None

	def _prefix(self):
		if self.account_id is None:self.account_id=str(self.api.get_account().id)
		return 'gc4h:'+self.account_id+':'

	def _lookup(self, client_id):
		try:return self.api.get_order_by_client_order_id(client_id)
		except Exception as exc:
			if getattr(exc,'status_code',None)==404:return None
			raise

	def _position(self,ticker):
		for p in self.api.list_positions():
			if p.symbol==ticker:
				qty=float(p.qty)
				if qty<0 or str(getattr(p,'side','long'))=='short':raise ValueError('unsupported_short_position')
				return qty,str(p.qty)
		return 0.,'0'

	def _orders(self,ticker):
		# The API supports nested=False and a 500 order bound; reject truncation.
		orders=self.api.list_orders(status='open',limit=500)
		if len(orders)>=500:raise ValueError('Open-order enumeration may be truncated')
		return [o for o in orders if o.symbol==ticker]

	def _session(self,ticker):
		clock=self.api.get_clock();now=pd.Timestamp(clock.timestamp).tz_convert('America/New_York')
		if clock.is_open:return True,False
		if now.weekday()<5 and 4<=now.hour<20:
			# Closed-clock days can be holidays: verify today's session exists.
			if not self.api.get_calendar(start=now.date().isoformat(),end=now.date().isoformat()):return False,True
			return True,True
		return False,True

	def _quote(self,ticker):
		q=self.api.get_latest_quote(ticker)
		if not math.isfinite(float(q.ap)) or not math.isfinite(float(q.bp)) or q.ap<=0 or q.bp<=0 or q.bp>q.ap:
			raise ValueError('invalid_quote')
		if getattr(q,'t',None) is None or abs((pd.Timestamp.now(tz='UTC')-pd.Timestamp(q.t)).total_seconds())>120:
			raise ValueError('stale_quote')
		return float(q.ap),float(q.bp)

	@staticmethod
	def _limit(price,buy=True):
		step=Decimal('0.01') if price>=1 else Decimal('0.0001')
		return Decimal(str(price)).quantize(step,rounding=ROUND_UP if buy else ROUND_DOWN)

	def process(self,event):
		if not self.enabled:return {'reason':'live_orders_disabled'}
		action=signal_action(event)
		if not action:return {'reason':'non_actionable_signal'}
		ticker=event['ticker'];prefix=self._prefix();intent_key=prefix+'exit:'+ticker
		# Expensive data lookup stays outside the account/symbol lock.
		row={}
		if not self.r.get(intent_key) and (action == 'entry' or event.get('timeframe') == '1h' or event.get('signal') == 'death_cross'):
			as_of=min(pd.Timestamp(event['received_dt']),pd.Timestamp(event['dt']))
			row=self.history.snapshot(ticker,as_of)
		if action == 'exit' and not self.r.get(intent_key):
			if evaluate_signal_exit(event,row,self.settings) is None:
				return {'reason':'exit_condition_not_met_or_missing_signal_price'}
		lock=self.r.lock(prefix+'lock:'+ticker,timeout=90,thread_local=False)
		if not lock.acquire(blocking=False):return {'reason':'account_symbol_busy'}
		stop=threading.Event();lost=threading.Event()
		def renew():
			while not stop.wait(20):
				try:
					if not lock.extend(90,replace_ttl=True):lost.set();return
				except Exception:lost.set();return
		thread=threading.Thread(target=renew,daemon=True);thread.start()
		def check_lock():
			if lost.is_set() or not lock.owned():raise RuntimeError('Account lock lost; refusing order submission')
		try:
			key_id=hashlib.sha256(repr(event_identity(event,action)).encode()).hexdigest()[:32]
			client_id='gc4h-'+key_id
			done_key=prefix+'event:'+key_id
			exit_bar_key=prefix+'exitbar:'+hashlib.sha256(repr(event_identity(event,'exit')).encode()).hexdigest()[:32]
			intent_raw=self.r.get(intent_key)
			if action=='exit' and not intent_raw and not self.r.get(done_key):
				intent={'client_id':client_id,'attempt':0,'done_key':done_key}
				check_lock();self.r.set(intent_key,json.dumps(intent));self.r.set(exit_bar_key,'1')
				intent_raw=json.dumps(intent)
			if intent_raw:
				return self._finish_exit(ticker,intent_key,json.loads(intent_raw),check_lock)
			if self.r.get(done_key) or self.r.get(exit_bar_key):return {'reason':'duplicate_or_exited_bar'}
			existing_order=self._lookup(client_id)
			if existing_order:
				self.r.set(done_key,'order:'+str(existing_order.id));return {'reason':'already_submitted','order_id':str(existing_order.id)}
			allowed,extended=self._session(ticker)
			if not allowed:return {'reason':'outside_supported_order_session'}
			asset=self.api.get_asset(ticker)
			if not asset.tradable:return {'reason':'asset_not_tradable'}
			fractional=bool(asset.fractionable)
			if extended and fractional and not getattr(asset,'fractional_eh_enabled',False):return {'reason':'fractional_extended_hours_not_enabled'}
			ask,bid=self._quote(ticker);price=float(self._limit(ask))
			orders=self._orders(ticker)
			if any(o.side=='sell' for o in orders):return {'reason':'closing_order_pending'}
			pending=0.
			for order in orders:
				if order.side!='buy':continue
				remaining=max(0.,float(getattr(order,'qty',0) or 0)-float(getattr(order,'filled_qty',0) or 0))
				if getattr(order,'notional',None):
					# Conservatively reserve the entire notional until this order is terminal.
					pending+=float(order.notional)
				elif getattr(order,'limit_price',None):pending+=remaining*max(price,float(order.limit_price))
				else:return {'reason':'unbounded_pending_buy_exposure'}
			qty,_=self._position(ticker)
			decision=self.engine(price,self.base_amount,row,qty*price,pending,self.settings)
			if not decision['eligible']:return decision
			quantity=Decimal(str(decision['quantity'])).quantize(Decimal('0.000000001') if fractional else Decimal('1'),rounding=ROUND_DOWN)
			if quantity<=0 or quantity*Decimal(str(price))<Decimal('1'):return dict(decision,reason='below_broker_minimum')
			check_lock()
			order=self.api.submit_order(symbol=ticker,qty=str(quantity),side='buy',type='limit',time_in_force='day',limit_price=str(self._limit(ask)),extended_hours=extended,client_order_id=client_id)
			self.r.set(done_key,'order:'+str(order.id))
			return dict(decision,reason='submitted',order_id=str(order.id),submitted_quantity=str(quantity),limit_price=price)
		finally:
			stop.set();thread.join(timeout=2)
			if lock.owned():lock.release()

	def _finish_exit(self,ticker,intent_key,intent,check_lock):
		for order in self._orders(ticker):
			if order.side=='buy':check_lock();self.api.cancel_order(order.id)
		if any(o.side=='buy' for o in self._orders(ticker)):
			return {'reason':'waiting_for_buy_cancellation'}
		client_id=intent['client_id']+'-'+str(intent['attempt'])
		order=self._lookup(client_id)
		if order and str(order.status) not in TERMINAL:return {'reason':'close_pending','order_id':str(order.id)}
		if order:
			intent['attempt']+=1;client_id=intent['client_id']+'-'+str(intent['attempt'])
			self.r.set(intent_key,json.dumps(intent))
		if any(o.side=='sell' for o in self._orders(ticker)):return {'reason':'waiting_for_existing_sell'}
		# Refresh only after all earlier sales are confirmed terminal. A fill can
		# occur during order lookup; using a pre-lookup quantity could open a short.
		qty,quantity=self._position(ticker)
		if qty<=0:
			self.r.set(intent['done_key'],'closed');self.r.delete(intent_key);return {'reason':'flat'}
		allowed,extended=self._session(ticker)
		if not allowed:return {'reason':'close_waiting_for_supported_session'}
		asset=self.api.get_asset(ticker)
		if extended and Decimal(quantity)%1 and not getattr(asset,'fractional_eh_enabled',False):return {'reason':'close_waiting_for_fractional_session'}
		ask,bid=self._quote(ticker);check_lock()
		order=self.api.submit_order(symbol=ticker,qty=quantity,side='sell',type='limit',time_in_force='day',limit_price=str(self._limit(bid,False)),extended_hours=extended,client_order_id=client_id)
		return {'reason':'close_submitted','order_id':str(order.id),'quantity':quantity}

	def reconcile_exits(self):
		if not self.enabled:return []
		prefix=self._prefix();results=[]
		for position in self.api.list_positions():
			if str(position.side) != 'long': continue
			ticker = position.symbol
			now = pd.Timestamp.now(tz='UTC')
			try:
				row = self.history.snapshot(ticker,now)
				if row.get('death_cross'):
					results.append(self.process({'ticker':ticker,'timeframe':'4h','signal':'death_cross','dt':pd.Timestamp(row['completed_at']),'received_dt':now}))
			except Exception:
				logger.exception('Death-cross monitoring failed for %s',ticker)
		for key in self.r.scan_iter(match=prefix+'exit:*'):
			if isinstance(key,bytes):key=key.decode()
			ticker=key[len(prefix+'exit:'):]
			now=pd.Timestamp.now(tz='UTC')
			try:
				results.append(self.process({'ticker':ticker,'timeframe':'4h','signal':'bullish_exit','dt':now,'received_dt':now}))
			except Exception as exc:
				logger.exception('Golden-cross exit reconciliation failed for %s; continuing other symbols',ticker)
				results.append({'ticker':ticker,'reason':'reconciliation_error','error':str(exc)})
		return results
