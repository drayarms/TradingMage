"""Long-horizon simulation adapter; reuses TradingMage's fills and PnL accounting."""
import math
import os
import pandas as pd
from golden_cross_dip_recovery import STRATEGY_NAME, snapshot_at, evaluate_entry, evaluate_signal_exit
from golden_cross_history import GoldenCrossHistory


def signal_action(event):
	raw = event.get('raw_fields') or {}
	try:
		bullish_exit = float(raw.get('bullish_exit') or 0) != 0
	except (ValueError, TypeError):
		bullish_exit = False
	signal = str(event.get('signal') or '').lower()
	if event.get('timeframe') in {'1h','4h'} and (bullish_exit or signal == 'bullish_exit' or (event.get('timeframe') == '4h' and signal == 'death_cross')):
		return 'exit'
	return 'entry' if event.get('timeframe') == '1h' and signal in {'buy','buy+','buy_plus'} else None


def event_identity(event, action):
	return (event['ticker'], str(event.get('dt') or event.get('time')), action)


def process_event(bt, state, event, base, settings, frames, seen, diagnostics):
	action = signal_action(event)
	if not action:
		return
	key = event_identity(event,action)
	if key in seen or (action == 'entry' and event_identity(event,'exit') in seen):
		return
	seen.add(key)
	position = state.positions.get(event['ticker'])
	if position and position.side != 'long':
		diagnostics.append({'ticker':event['ticker'],'time':event['received_dt'].isoformat(),'reason':'unsupported_short_position'})
		return
	if action == 'exit':
		as_of = min(pd.Timestamp(event['received_dt']), pd.Timestamp(event['dt']))
		row = snapshot_at(frames.get(event['ticker'], pd.DataFrame()), as_of)
		reason = evaluate_signal_exit(event,row,settings)
		if reason is None:
			diagnostics.append({'ticker':event['ticker'],'time':event['received_dt'].isoformat(),'reason':'exit_condition_not_met_or_missing_signal_price'}); return
		signal_time = event['time']
		event = dict(event, time=event['received_dt'].isoformat(), exit_reason=reason)
		previous_count = len(state.trade_events)
		bt._close_position(state,event)
		if len(state.trade_events)>previous_count:
			state.trade_events[-1]['signal_time']=signal_time
			state.trade_events[-1]['exit_reason']=reason
		return
	try:
		quote = bt._get_simulated_quote(state,event)
		price = bt._get_execution_price(quote,'buy')
	except ValueError as exc:
		diagnostics.append({'ticker':event['ticker'],'time':event['received_dt'].isoformat(),'reason':str(exc)})
		return
	# Delayed delivery cannot use indicator context later than the signal candle.
	as_of = min(pd.Timestamp(event['received_dt']),pd.Timestamp(event['dt']))
	row = snapshot_at(frames.get(event['ticker'],pd.DataFrame()),as_of)
	engine = getattr(getattr(bt,"strategies_instance",None),"evaluate_golden_cross_dip_recovery",evaluate_entry)
	decision = engine(price,base,row,(position.num_shares*price if position else 0),0,settings)
	diagnostics.append(dict(decision,ticker=event['ticker'],time=event['received_dt'].isoformat(),price=price))
	if decision['eligible']:
		event = dict(event,exit_strategy=None)
		bt._open_or_add_position(state,event,'long',decision['quantity'])


def run_simulation(bt, api, start, end, tickers, position_size, settings, include_news_sentiment=False):
	from backtester import SimState
	start_dt,end_dt = bt._parse_input_dt(start),bt._parse_input_dt(end)
	if start_dt > end_dt:
		raise ValueError('start must be <= end')
	base = float(position_size if position_size is not None else os.getenv('POSITION_SIZE_1H','6600'))
	if not math.isfinite(base) or base <= 0:
		raise ValueError('position_size must be finite and > 0')
	# Load all observational timeframes while strategy decisions act only on 4H.
	timeframes={'1m','5m','15m','1h','4h'}
	symbols = bt._normalize_tickers(tickers) or bt._discover_tickers({'1h','4h'})
	if not symbols:
		raise ValueError('No tickers or 4H signal streams found')
	events=bt._load_signal_events(STRATEGY_NAME,symbols,timeframes,start_dt,end_dt)
	events.sort(key=lambda e:e['received_dt'])
	prices=bt.trade_records_instance.get_df(api,symbols,bt.trade_records_instance._1min_time_frame,pd.Timestamp(start_dt)-pd.Timedelta(minutes=10),pd.Timestamp(end_dt))
	if prices.empty:
		raise ValueError('Alpaca returned no one-minute execution/valuation data')
	history=GoldenCrossHistory(api,settings,bt.trade_records_instance._4hr_time_frame)
	frames={ticker:history.get_frame(ticker,start_dt,end_dt) for ticker in symbols}
	state=SimState()
	state.market_data={f'{c}_1m':bt.trade_records_instance.dataframe_column_to_dict(prices,c) for c in ('open','high','low','close')}
	state.market_data.update(anchor_atr={},anchor_ohlc={},market_close_liquidation_times=[])
	seen=set();diagnostics=[]
	bt.recording_enabled=True;bt.diagnostic_logging_enabled=False
	timeline=bt._build_backtest_timeline(state,events,start_dt,end_dt,market_bar_interval_minutes=bt.PNL_SNAPSHOT_INTERVAL_MINUTES)
	for ticker, frame in frames.items():
		if 'death_cross' not in frame: continue
		for _, row in frame.loc[frame.death_cross & (frame.completed_at >= pd.Timestamp(start_dt)) & (frame.completed_at <= pd.Timestamp(end_dt))].iterrows():
			timeline.append({'kind':'death_cross','dt':row.completed_at.to_pydatetime(),'payload':{'ticker':ticker,'price':float(row.close)}})
	timeline.sort(key=lambda item:(item['dt'], {'market_bar':0,'death_cross':1,'signal':2}.get(item['kind'],3)))
	for item in timeline:
		if item['kind']=='market_bar':
			payload=item['payload'];state.last_price_by_ticker[payload['ticker']]=float(payload['close'])
			bt._record_snapshots(state,item['dt'])
		elif item['kind']=='death_cross':
			ticker = item['payload']['ticker']
			position = state.positions.get(ticker)
			if position and position.side == 'long':
				price = state.last_price_by_ticker.get(ticker,item['payload']['price'])
				bt._close_position_at_market_bar(state,ticker,item['dt'],price,'4h_death_cross')
				bt._record_snapshots(state,item['dt'])
		elif item['kind']=='signal':
			event=item['payload'];bt._register_event_context(state,event)
			if state.last_exit_time_by_ticker.get(event['ticker']) == event['received_dt']:
				continue
			process_event(bt,state,event,base,settings,frames,seen,diagnostics)
			bt._record_snapshots(state,event['received_dt'])
	# End-of-period valuation uses the latest completed minute; never liquidates.
	for ticker in symbols:
		rows=state.market_data['close_1m'].get(ticker,{})
		valid=[(pd.Timestamp(t),float(p)) for t,p in rows.items() if pd.Timestamp(t)+pd.Timedelta(minutes=1)<=pd.Timestamp(end_dt)]
		if valid: state.last_price_by_ticker[ticker]=max(valid,key=lambda x:x[0])[1]
	bt._record_snapshots(state,end_dt)
	anchor={};indicators={}
	for ticker,frame in frames.items():
		visible=frame.loc[(frame.index>=pd.Timestamp(start_dt)) & (frame.completed_at<=pd.Timestamp(end_dt))] if not frame.empty else frame
		anchor[ticker]=[{'time':t.isoformat(),**{c:float(row[c]) for c in ('open','high','low','close')},'volume':float(row.get('volume',0))} for t,row in visible.iterrows()]
		indicators[ticker]=[{'time':t.isoformat(),'completed_at':row.completed_at.isoformat(),**{k:(None if pd.isna(row[k]) else float(row[k])) for k in ('fast_moving_average','slow_moving_average')}} for t,row in visible.iterrows()]
	news=[]
	if include_news_sentiment:
		for ticker in symbols:news.extend(bt.alpha_vantage_news.get_news_sentiment(ticker=ticker,start_dt=start_dt,end_dt=end_dt))
	open_positions=[]
	for ticker,p in state.positions.items():
		market=state.last_price_by_ticker.get(ticker,p.avg_price_per_share)
		open_positions.append({'ticker':ticker,'side':'long','num_shares':p.num_shares,'avg_price_per_share':p.avg_price_per_share,'market_price':market,'unrealized_pnl':(market-p.avg_price_per_share)*p.num_shares,'market_value':market*p.num_shares})
	return {'strategy_name':STRATEGY_NAME,'exit_strategy':'death_cross_or_4h_bullish_exit_or_1h_upper_atr_bullish_exit','anchor_timeframe':'4h','ATR_period':settings.atr_period,'ATR_multiplier':None,'start':start_dt.isoformat(),'end':end_dt.isoformat(),'position_size':base,'strategy_settings':settings.to_dict(),'warmup_sessions':0,'warmup_signal_count':0,'signal_count':len(events),'tickers':symbols,'signal_events':[bt._serialize_signal_event(e) for e in events],'trade_events':state.trade_events,'trade_count':sum(e['event_type'] in {'open','add','close'} for e in state.trade_events),'overall_pnl_history':state.overall_pnl_history,'ticker_pnl_history':state.ticker_pnl_history,'daily_max_open_exposure':bt._daily_exposure_rows(state.daily_max_exposure),'anchor_bars':anchor,'indicator_series':indicators,'news_sentiment_events':news,'include_news_sentiment':include_news_sentiment,'open_positions':open_positions,'realized_pnl':sum(state.realized_by_ticker.values()),'unrealized_pnl':sum(p['unrealized_pnl'] for p in open_positions),'entry_diagnostics':diagnostics,'liquidate_before_market_close':False,'run_exit_strategy':True}
