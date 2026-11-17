"""Live virtual strategy ledger plus separately reconciled Alpaca net inventory."""
import copy,json,logging,threading
from dataclasses import asdict
from datetime import datetime
import pandas as pd
from backtest_sessions import BacktestSessions
from golden_cross_history import GoldenCrossHistory
from cross_strategy_indicators import fast_moving_average_reversed
from cross_entry_policy import attach_cross_turning_context
from range_strategy import attach_turning_point_gap_reference,attach_post_stretch_context
from trend_strategy import prepare_strategy_frame
from range_trend_portfolio import RangeTrendPortfolio
from range_trend_netting import NetBroker

logger=logging.getLogger('tv-webhook')

def zone_identity(active,reconstructed,previous):
	if not active:return None
	# A reconstructed start after the last observed anchor proves a new zone,
	# even when both the saved and latest observations are active.
	if previous.get('active') and previous.get('id') and previous.get('completed_at'):
		if reconstructed is None or pd.Timestamp(reconstructed)<=pd.Timestamp(previous['completed_at']):return previous['id']
	return reconstructed

class RangeTrendLive:
	def __init__(self,bt,data_api,broker,redis,settings,options,base,enabled=False,tickers=()):
		self.bt,self.data_api,self.api,self.r,self.settings,self.options,self.base=bt,data_api,broker,redis,settings,options,base
		self.enabled=enabled;self.prefix=None;self.tickers=set(tickers)
	def namespace(self):
		if self.prefix is None:self.prefix='range_trend_4h:'+str(self.api.get_account().id)+':'
		return self.prefix
	def process(self,event):
		if not self.enabled:return dict(status='live_orders_disabled')
		self.r.sadd(self.namespace()+'symbols',event['ticker'])
		return dict(status='symbol_registered_for_minute_evaluation')
	def reconcile_exits(self):
		if not self.enabled:return []
		prefix=self.namespace();lock=self.r.lock(prefix+'account_lock',timeout=120,blocking_timeout=0,thread_local=False)
		if not lock.acquire(blocking=False):return [dict(status='account_busy')]
		stop=threading.Event();lost=threading.Event()
		def renew():
			while not stop.wait(30):
				try:lock.extend(120,replace_ttl=True)
				except Exception:lost.set();return
		def check():
			if lost.is_set() or not lock.owned():raise RuntimeError('Account execution lock lost')
		thread=threading.Thread(target=renew,daemon=True);thread.start();results=[]
		try:
			now=pd.Timestamp.now(tz='UTC')
			for ticker in self.tickers:self.r.sadd(prefix+'symbols',ticker)
			if not self.r.smembers(prefix+'symbols'):
				for ticker in self.bt._discover_tickers({'1h','4h'}):self.r.sadd(prefix+'symbols',ticker)
			net=NetBroker(self.api,self.r,prefix,check)
			for symbol in sorted(self.r.smembers(prefix+'symbols')):
				ticker=symbol.decode() if isinstance(symbol,bytes) else symbol
				try:
					check();target=self.evaluate(ticker,now,check)
					if target is not None:results.append(dict(ticker=ticker,**net.reconcile(ticker,target,now)))
				except Exception:
					logger.exception('Range/trend evaluation or net reconciliation failed for %s',ticker)
					results.append(dict(ticker=ticker,status='reconciliation_error'))
			return results
		finally:
			stop.set();thread.join(timeout=2)
			if lock.owned():lock.release()
	def evaluate(self,ticker,now,check):
		from backtester import SimState,SimPosition
		prefix=self.namespace();key=prefix+'ledger:'+ticker;raw=self.r.get(key);saved=json.loads(raw) if raw else {}
		calendar=BacktestSessions.load(self.data_api,now,now)
		if not calendar.allows(now):return None
		# Refuse takeover of existing broker inventory or unrelated orders.
		netter=NetBroker(self.api,self.r,prefix,check)
		if not raw and (netter.position(ticker)!=0 or self.api.list_orders(status='open',symbols=[ticker])):return None
		minute=self.data_api.get_bars(ticker,self.bt.trade_records_instance._1min_time_frame,start=(now-pd.Timedelta(minutes=10)).isoformat(),end=now.isoformat(),adjustment='raw').df.copy()
		if isinstance(minute.index,pd.MultiIndex):minute.index=minute.index.get_level_values('timestamp')
		minute.index=pd.to_datetime(minute.index,utc=True);minute=minute.loc[minute.index+pd.Timedelta(minutes=1)<=now].sort_index()
		if minute.empty:return saved.get('net_target')
		completed=minute.index[-1]+pd.Timedelta(minutes=1)
		if now-completed>pd.Timedelta(seconds=90) or not calendar.same_session(minute.index[-1],now):return None
		if saved.get('evaluated_minute')==completed.isoformat():return saved['net_target']
		history=GoldenCrossHistory(self.data_api,self.settings,self.bt.trade_records_instance._4hr_time_frame)
		f=history.get_frame(ticker,now-pd.Timedelta(days=1),now)
		if f.empty:return None
		points=fast_moving_average_reversed(f,'4h');f=attach_cross_turning_context(attach_turning_point_gap_reference(attach_post_stretch_context(f),points),points)
		f=prepare_strategy_frame(f,points,self.options)
		# Preserve zone identity across rolling-history refreshes and process restarts.
		zones=saved.get('zone_state',{})
		for field in ('universal_peak','universal_trough'):
			active=bool(f.iloc[-1][field]);previous=zones.get(field,{})
			reconstructed=f.iloc[-1].get(field+'_zone_id') if active else None
			current_completed=pd.Timestamp(f.iloc[-1].completed_at).isoformat()
			identity=zone_identity(active,reconstructed,previous)
			zones[field]=dict(active=active,id=identity,completed_at=current_completed);f.loc[f.index[-1],field+'_zone_id']=identity
		quote=self.api.get_latest_quote(ticker);stamp=pd.Timestamp(quote.timestamp)
		if stamp.tzinfo is None or not 0<=(now-stamp).total_seconds()<=90:return None
		bid,ask=float(quote.bid_price),float(quote.ask_price)
		if not 0<bid<=ask:return None
		adapter=copy.copy(self.bt);adapter.recording_enabled=True;adapter.diagnostic_logging_enabled=False
		adapter._get_simulated_quote=lambda state,event:dict(market=float(minute.close.iloc[-1]),bid=bid,ask=ask,source_bar_time=minute.index[-1])
		s=SimState(last_price_by_ticker={ticker:float(minute.close.iloc[-1])});s.market_data=dict(close_1m={},volume_1m={},execution_sessions=calendar)
		p=RangeTrendPortfolio(adapter,self.settings,s,{ticker:f},self.base)
		# These are virtual ledger fills; only NetBroker submits real broker orders.
		p.limit_orders.submit=lambda *args,**kwargs:False
		for owner,data in saved.get('books',{}).items():
			if owner not in p.books:continue
			book=p.books[owner]
			if data.get('position'):
				fields=data['position']
				for name in ('entry_time','entry_signal_time','trailing_stop_source_time','last_trailing_bar_time','trailing_stop_expanded_at'):
					if fields.get(name):fields[name]=pd.Timestamp(fields[name]).to_pydatetime()
				book.positions[ticker]=SimPosition(**fields)
			book.realized_by_ticker[ticker]=data.get('realized',0.)
			if data.get('last_exit'):book.last_exit_time_by_ticker[ticker]=pd.Timestamp(data['last_exit']).to_pydatetime()
			if data.get('last_fill') and ticker in book.positions:p.last_heikin_ashi_entry[owner,ticker,book.positions[ticker].side]=(pd.Timestamp(data['last_fill']),book.positions[ticker])
		for name,identity in saved.get('last_trend_reversal',{}).items():p.last_trend_reversal[name,ticker]=identity
		for name,visit in saved.get('trend_band_visits',{}).items():
			if name in p.books and ticker in p.books[name].positions:p.trend_band_visits[name,ticker]=visit
		for side,stamp in saved.get('last_range_entry',{}).items():p.last_range_entry[ticker,side]=int(p.times[ticker].searchsorted(pd.Timestamp(stamp).value,side='right'))-1
		for owner,event in saved.get('blocked_exits',{}).items():
			position=p.books[owner].positions.get(ticker)
			if position is not None:
				for name in ('received_dt','dt','sort_dt'):
					if event.get(name):event[name]=pd.Timestamp(event[name]).to_pydatetime()
				p.blocked_exit_intents[owner,ticker]=(event,position)
		from heikin_ashi_filter import load_hourly_runs
		p.heikin_ashi_runs=load_hourly_runs(self.data_api,self.settings,[ticker],now-pd.Timedelta(days=7),now,{ticker:f})
		when=now.to_pydatetime();p.process_heikin_ashi_exits(ticker,when);p.process_zone_exits(ticker,when)
		p.process_exit_intents(when);p.process_range(ticker,when,False);p.process_trend(ticker,when,False)
		for family in (('trend','range') if self.settings.entry_strategy_precedence=='trend' else ('range','trend')):
			if family=='trend':p.process_trend(ticker,when,True)
			else:p.process_range(ticker,when,True)
		p.record(when)
		books={}
		for owner,book in p.books.items():
			if owner=='cross':continue
			position=book.positions.get(ticker);last=p.last_heikin_ashi_entry.get((owner,ticker,position.side)) if position else None
			books[owner]=dict(position=asdict(position) if position else None,realized=book.realized_by_ticker.get(ticker,0.),last_exit=book.last_exit_time_by_ticker.get(ticker),last_fill=last[0] if last else None)
		payload=dict(blocked_exits={owner:event for (owner,t),(event,position) in p.blocked_exit_intents.items()},books=books,net_target=p.net_positions.get(ticker,0.),evaluated_minute=completed.isoformat(),zone_state=zones,last_trend_reversal={owner:value for (owner,t),value in p.last_trend_reversal.items()},trend_band_visits={owner:value for (owner,t),value in p.trend_band_visits.items()},last_range_entry={side:p.rows[ticker][index]['completed_at'] for (t,side),index in p.last_range_entry.items() if 0<=index<len(p.rows[ticker])})
		check();self.r.set(key,json.dumps(payload,default=lambda v:v.isoformat() if hasattr(v,'isoformat') else str(v),allow_nan=False))
		for event in p.aggregate.trade_events:self.r.xadd(prefix+'virtual_fills',{k:json.dumps(v,default=str) for k,v in event.items()})
		return payload['net_target']
