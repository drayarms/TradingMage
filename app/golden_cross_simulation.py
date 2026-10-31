"""Long-horizon simulation adapter; reuses TradingMage's fills and PnL accounting."""
import math
from itertools import groupby
from strategy_portfolio import StrategyPortfolio
from market_filters import confirmation_candle_exit, MARKET_FIELDS
from backtest_progress import BacktestProgress
import os
import pandas as pd
from golden_cross_dip_recovery import STRATEGY_NAME, snapshot_at, evaluate_entry, evaluate_signal_exit, evaluate_short_entry, evaluate_short_signal_exit
from golden_cross_history import GoldenCrossHistory
from cross_strategy_indicators import evaluate_cross_exit, fast_moving_average_reversed, REVERSAL_TIMEFRAMES, turning_point_exit_reasons


def signal_action(event, side="long"):
	if side == "short":
		return short_signal_action(event)
	if event.get('signal') == '_cross_exit': return 'exit'
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


def short_signal_action(event):
	if event.get('signal') == '_cross_exit': return 'exit'
	raw = event.get('raw_fields') or {}
	try:
		bearish = float(raw.get('bearish_exit') or 0) != 0
	except (ValueError, TypeError):
		bearish = False
	signal = str(event.get('signal') or '').lower()
	if event.get('timeframe') in {'1h', '4h'} and (bearish or signal == 'bearish_exit' or (event.get('timeframe') == '4h' and signal == 'golden_cross')):
		return 'exit'
	return 'entry' if event.get('timeframe') == '1h' and signal in {'sell', 'sell+', 'sell_plus'} else None


def selected_side(event, settings, position_side=None):
	# Existing positions determine which exit rules apply. Never open an opposite
	# position before the first one has closed, even with both modes enabled.
	if position_side:
		return position_side
	if settings.strategy_mode == 'golden_cross': return 'long'
	if settings.strategy_mode == 'death_cross': return 'short'
	if signal_action(event, 'short') in {'entry', 'exit'} and signal_action(event, 'long') != 'exit': return 'short'
	return 'long'


def process_event(bt, state, event, base, settings, frames, seen, diagnostics):
	position = state.positions.get(event['ticker'])
	marker_side=event.get('_turning_point_entry_side')
	if marker_side and position and position.side != marker_side: return
	side = marker_side or selected_side(event, settings, position.side if position else None)
	turning_exit = settings.exit_after_turning_point and event.get('_turning_point_exit_side') == side
	action = 'exit' if turning_exit else 'entry' if marker_side else signal_action(event, side)
	if action == 'entry' and ((settings.entry_trigger == 'turning_point') != bool(marker_side)): return
	if not action: return
	key = event_identity(event, action)
	if key in seen or (action == 'entry' and event_identity(event, 'exit') in seen): return
	seen.add(key)
	as_of = min(pd.Timestamp(event['received_dt']), pd.Timestamp(event['dt']))
	row = snapshot_at(frames.get(event['ticker'], pd.DataFrame()), as_of)
	if row:
		row['_market_filter_context'] = snapshot_at(frames.get(event['ticker'], pd.DataFrame()),event['received_dt'])
	direction = 'golden_cross' if side == 'long' else 'death_cross'
	previous_count = len(state.trade_events)
	if action == 'exit':
		engine = evaluate_signal_exit if side == 'long' else evaluate_short_signal_exit
		reason = evaluate_cross_exit(side, event.get('price'), row, settings) if event.get('signal') == '_cross_exit' else engine(event, row, settings)
		if turning_exit: reason = event['timeframe']+('_bearish_exit_after_maximum' if side == 'long' else '_bullish_exit_after_minimum')
		if reason is None:
			diagnostics.append({'ticker':event['ticker'], 'time':event['received_dt'].isoformat(), 'strategy_direction':direction, 'reason':'exit_condition_not_met_or_missing_signal_price'}); return
		signal_time = event['time']
		bt._close_position(state, dict(event, time=event['received_dt'].isoformat(), exit_reason=reason))
		if len(state.trade_events) > previous_count:
			state.trade_events[-1].update(signal_time=signal_time, exit_reason=reason, strategy_direction=direction)
		return
	if settings.strategy_mode not in {direction, 'both'}: return
	if hasattr(state,'entry_guard') and not state.entry_guard(event['ticker'],side):
		diagnostics.append(dict(ticker=event['ticker'],time=event['received_dt'].isoformat(),reason='opposite_position_in_shared_book',strategy_direction=direction)); return
	try:
		quote = bt._get_simulated_quote(state, event)
		price = bt._get_execution_price(quote, 'buy' if side == 'long' else 'sell')
	except ValueError as exc:
		diagnostics.append({'ticker':event['ticker'], 'time':event['received_dt'].isoformat(), 'reason':str(exc)}); return
	engine = getattr(getattr(bt, 'strategies_instance', None), 'evaluate_golden_cross_dip_recovery', evaluate_entry) if side == 'long' else getattr(getattr(bt, 'strategies_instance', None), 'evaluate_death_cross_dip_recovery', evaluate_short_entry)
	decision = engine(price, base, row, position.num_shares*price if position else 0, 0, settings)
	diagnostics.append(dict(decision, ticker=event['ticker'], time=event['received_dt'].isoformat(), price=price, strategy_direction=direction))
	if decision['eligible']:
		bt._open_or_add_position(state, dict(event, exit_strategy=None), side, decision['quantity'])
		for trade in state.trade_events[previous_count:]:
			trade.update(strategy_direction=direction,entry_trigger=settings.entry_trigger)
			if marker_side: trade['turning_point_evaluated_at']=event['turning_point_evaluated_at']
		if settings.exit_on_confirmatory_candlestick and not position and event['ticker'] in state.positions:
			if not hasattr(state,'confirmatory_pairs'): state.confirmatory_pairs={}
			state.confirmatory_pairs[event['ticker']]={'position':state.positions[event['ticker']],'opened_at':event['received_dt']}


def directional_snapshot(state, when, reports):
	for direction, side in (('golden_cross', 'long'), ('death_cross', 'short')):
		report = reports[direction]
		realized = sum(float(e.get('realized_delta') or 0) for e in state.trade_events if e.get('strategy_direction') == direction and e.get('event_type') == 'close')
		unrealized = exposure = 0.
		for ticker, position in state.positions.items():
			if position.side != side: continue
			price = state.last_price_by_ticker.get(ticker, position.avg_price_per_share)
			unrealized += (price-position.avg_price_per_share)*position.num_shares*(1 if side == 'long' else -1)
			exposure += abs(price*position.num_shares)
		report.update(realized_pnl=realized, unrealized_pnl=unrealized, total_pnl=realized+unrealized)
		report['overall_pnl_history'].append({'time':when.isoformat(), 'overall_total_pnl':realized+unrealized, 'gross_open_exposure':exposure})


def run_simulation(bt, api, start, end, tickers, position_size, settings):
	from backtester import SimState
	progress=BacktestProgress()
	progress.stage("Loading signals and market data...")
	start_dt,end_dt = bt._parse_input_dt(start),bt._parse_input_dt(end)
	if start_dt > end_dt:
		raise ValueError('start must be <= end')
	base = float(position_size if position_size is not None else os.getenv('POSITION_SIZE_1H','6600'))
	if not math.isfinite(base) or base <= 0:
		raise ValueError('position_size must be finite and > 0')
	# Keep 1H/4H signal context for exits; indicator-based entry context is 4H.
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
	frames={}
	for number,ticker in enumerate(symbols,1):
		progress.stage(f'Loading indicators: {ticker} ({number}/{len(symbols)})')
		frames[ticker]=history.get_frame(ticker,start_dt,end_dt)
	points={}
	if settings.exit_after_turning_point or settings.entry_trigger == 'turning_point':
		# Execution always uses 4H turning points, independently of plot switches.
		points={ticker:fast_moving_average_reversed(frame,'4h') for ticker,frame in frames.items()}
	if settings.exit_after_turning_point:
		for event,side in zip(events,turning_point_exit_reasons(events,points)):
			if side: event['_turning_point_exit_side']=side
	state=SimState()
	state.market_data={f'{c}_1m':bt.trade_records_instance.dataframe_column_to_dict(prices,c) for c in ('open','high','low','close')}
	state.market_data.update(anchor_atr={},anchor_ohlc={},market_close_liquidation_times=[])
	seen=set();diagnostics=[]
	portfolio=StrategyPortfolio(bt,settings,state,frames,base)
	state.entry_guard=lambda ticker,side:portfolio.allows('cross',ticker,side)
	reports=portfolio.direction_reports
	def record(when):
		portfolio.record(when)
	bt.recording_enabled=True;bt.diagnostic_logging_enabled=False
	timeline=bt._build_backtest_timeline(state,events,start_dt,end_dt,market_bar_interval_minutes=(1 if portfolio.active('range') or settings.cross_exit_method == 'price' else bt.PNL_SNAPSHOT_INTERVAL_MINUTES))
	context_times={ticker:pd.DatetimeIndex(frame.completed_at).asi8 for ticker,frame in frames.items() if not frame.empty}
	if settings.exit_on_confirmatory_candlestick:
		by_candle={ticker:{} for ticker in frames}
		for event in events:
			ticker=event['ticker']
			if ticker not in context_times or event.get('timeframe') not in {'1h','4h'}: continue
			index=int(context_times[ticker].searchsorted(pd.Timestamp(event['received_dt']).value,side='left'))
			if index<len(frames[ticker]) and frames[ticker].index[index]<pd.Timestamp(event['received_dt']):
				by_candle[ticker].setdefault(index,[]).append(event)
		for ticker,frame in frames.items():
			for index in range(1,len(frame)):
				when=pd.Timestamp(frame.iloc[index].completed_at)
				if not pd.Timestamp(start_dt)<=when<=pd.Timestamp(end_dt): continue
				timeline.append(dict(kind='confirmation_candle',dt=when.to_pydatetime().astimezone(start_dt.tzinfo),payload=dict(ticker=ticker,index=index)))

	if settings.entry_trigger == 'turning_point':
		for ticker,markers in points.items():
			for point in markers:
				when=pd.Timestamp(point['confirmed_at'])
				if not pd.Timestamp(start_dt)<=when<=pd.Timestamp(end_dt): continue
				when=when.to_pydatetime().astimezone(start_dt.tzinfo)
				side='long' if point['turning_point_type']=='minimum' else 'short'
				event=dict(ticker=ticker,timeframe='4h',signal=point['turning_point_type']+'_turning_point',
					dt=when,received_dt=when,sort_dt=when,time=when.isoformat(),received_time=when.isoformat(),
					stream_id='turning_point:'+ticker+':'+when.isoformat(),strategy_name=STRATEGY_NAME,
					signal_role='turning_point_entry',side='buy' if side=='long' else 'sell',raw_fields={},
					_turning_point_entry_side=side,turning_point_evaluated_at=point['time'])
				timeline.append(dict(kind='turning_point',dt=when,payload=event))

	for ticker, frame in frames.items():
		for crossing in (('dema5_death_cross', 'dema5_golden_cross') if settings.cross_exit_method == 'dema5' else ('death_cross', 'golden_cross') if settings.cross_exit_method == 'fast_moving_average' else ()):
			if crossing not in frame: continue
			for _, row in frame.loc[frame[crossing] & (frame.completed_at >= pd.Timestamp(start_dt)) & (frame.completed_at <= pd.Timestamp(end_dt))].iterrows():
				timeline.append({'kind':crossing, 'dt':row.completed_at.to_pydatetime(), 'payload':{'ticker':ticker, 'price':float(row.close)}})
	timeline.sort(key=lambda item:(item['dt'], {'market_bar':0, 'death_cross':1, 'golden_cross':1, 'dema5_death_cross':1, 'dema5_golden_cross':1, 'signal':2,'confirmation_candle':3}.get(item['kind'],4)))
	for when,items in groupby(timeline,key=lambda item:item['dt']):
		progress.update(when)
		batch=list(items)
		tickers_at_time=sorted({item['payload']['ticker'] for item in batch})
		before_trades=len(portfolio.aggregate.trade_events)
		for item in batch:
			if item['kind']=='market_bar':
				payload=item['payload'];state.last_price_by_ticker[payload['ticker']]=float(payload['close'])
		for ticker in tickers_at_time: portfolio.process_range(ticker,when,entries=False)
		if portfolio.active('cross'):
			for item in batch:
				if item['kind']=='market_bar' and settings.cross_exit_method=='price':
					ticker=item['payload']['ticker'];position=state.positions.get(ticker)
					index,row=portfolio.context(ticker,when)
					if position and row is not None:
						reason=evaluate_cross_exit(position.side,float(item['payload']['close']),row,settings)
						if reason: bt._close_position_at_market_bar(state,ticker,when,float(item['payload']['close']),reason)
				elif item['kind'] in {'death_cross','golden_cross','dema5_death_cross','dema5_golden_cross'}:
					ticker=item['payload']['ticker'];position=state.positions.get(ticker)
					if position and position.side==('long' if item['kind'].endswith('death_cross') else 'short'):
						bt._close_position_at_market_bar(state,ticker,when,state.last_price_by_ticker.get(ticker,item['payload']['price']),'4h_'+item['kind'])
				elif item['kind']=='confirmation_candle':
					ticker=item['payload']['ticker'];index=item['payload']['index'];position=state.positions.get(ticker)
					pair=getattr(state,'confirmatory_pairs',{}).get(ticker)
					if position and pair and pair['position'] is position:
						candidate_events=by_candle[ticker].get(index-1,[])+by_candle[ticker].get(index,[])
						reason=confirmation_candle_exit(frames[ticker].iloc[index-1:index+1],candidate_events,position.side,pair['opened_at'])
						if reason: bt._close_position_at_market_bar(state,ticker,when,state.last_price_by_ticker.get(ticker,float(frames[ticker].iloc[index].close)),reason)
				elif item['kind'] in {'signal','turning_point'}:
					event=item['payload']
					if item['kind']=='signal': bt._register_event_context(state,event)
					if state.last_exit_time_by_ticker.get(event['ticker'])!=event['received_dt']:
						count=len(diagnostics)
						process_event(bt,state,event,base,settings,frames,seen,diagnostics)
						for diagnostic in diagnostics[count:]: diagnostic.update(strategy_type='cross',is_virtual=not settings.cross_strategy_enabled)
				portfolio.sync('cross')
		for ticker in tickers_at_time: portfolio.process_range(ticker,when,entries=True)
		if when.minute%bt.PNL_SNAPSHOT_INTERVAL_MINUTES==0 or any(item['kind']!='market_bar' for item in batch) or len(portfolio.aggregate.trade_events)!=before_trades:
			record(when)
	progress.stage('Simulation complete; preparing results...')
	# End-of-period valuation uses the latest completed minute; never liquidates.
	for ticker in symbols:
		rows=state.market_data['close_1m'].get(ticker,{})
		valid=[(pd.Timestamp(t),float(p)) for t,p in rows.items() if pd.Timestamp(t)+pd.Timedelta(minutes=1)<=pd.Timestamp(end_dt)]
		if valid: state.last_price_by_ticker[ticker]=max(valid,key=lambda x:x[0])[1]
	record(end_dt)
	state=portfolio.aggregate
	diagnostics.extend(portfolio.diagnostics)
	anchor={};indicators={}
	for ticker,frame in frames.items():
		visible=frame.loc[(frame.index>=pd.Timestamp(start_dt)) & (frame.completed_at<=pd.Timestamp(end_dt))] if not frame.empty else frame
		anchor[ticker]=[{'time':t.isoformat(),**{c:float(row[c]) for c in ('open','high','low','close')},'volume':float(row.get('volume',0))} for t,row in visible.iterrows()]
		indicators[ticker]=[{'time':t.isoformat(),'completed_at':row.completed_at.isoformat(),**{k:(None if pd.isna(row.get(k)) else bool(row[k]) if k in MARKET_FIELDS[1:4] else float(row[k])) for k in ('fast_moving_average','slow_moving_average','bollinger_upper','bollinger_lower','dema5','atr','high','low',*MARKET_FIELDS)}} for t,row in visible.iterrows()]
	reversals={ticker:[] for ticker in symbols}
	if settings.fast_moving_average_reversed:
		reversal_frames=frames
		tf=settings.fast_moving_average_reversal_timeframe
		if tf != '4h':
			from alpaca_trade_api.rest import TimeFrame, TimeFrameUnit
			minutes=REVERSAL_TIMEFRAMES[tf]
			timeframe=TimeFrame(minutes//60, TimeFrameUnit.Hour) if minutes >= 60 else TimeFrame(minutes,TimeFrameUnit.Minute)
			reversal_history=GoldenCrossHistory(api,settings,timeframe,initial_days=14,bar_duration=pd.Timedelta(minutes=minutes))
			reversal_frames={ticker:reversal_history.get_frame(ticker,start_dt,end_dt) for ticker in symbols}
		for ticker, frame in reversal_frames.items():
			reversals[ticker]=[m for m in fast_moving_average_reversed(frame,tf) if pd.Timestamp(start_dt)<=pd.Timestamp(m['confirmed_at'])<=pd.Timestamp(end_dt) and pd.Timestamp(m['time'])>=pd.Timestamp(start_dt)]
	open_positions=portfolio.open_positions()
	for owner,report in portfolio.strategy_reports.items():
		report.setdefault('realized_pnl',0.)
		report.setdefault('unrealized_pnl',0.)
		report.setdefault('total_pnl',0.)
		report['trade_count']=sum(e.get('strategy_type')==owner and e.get('event_type') in {'open','add','close'} for e in state.trade_events)
	for direction, report in reports.items():
		report['trade_count'] = sum(e.get('strategy_direction') == direction and e.get('event_type') in {'open', 'add', 'close'} for e in state.trade_events)
	return {'strategy_results':portfolio.strategy_reports,'virtual_trade_events':portfolio.virtual_trade_events,'virtual_open_positions':portfolio.open_positions(virtual=True),'cross_exit_method':settings.cross_exit_method, 'fast_moving_average_reversed':reversals, 'strategy_mode':settings.strategy_mode, 'direction_results':reports, 'strategy_name':STRATEGY_NAME,'exit_strategy':('death_cross_or_4h_bullish_exit_or_1h_upper_atr_bullish_exit' if settings.cross_exit_method == 'fast_moving_average' and settings.strategy_mode == 'golden_cross' else settings.cross_exit_method+'_slow_average_or_directional_signal_exit'),'anchor_timeframe':'4h','ATR_period':settings.atr_period,'ATR_multiplier':None,'start':start_dt.isoformat(),'end':end_dt.isoformat(),'position_size':base,'strategy_settings':settings.to_dict(),'warmup_sessions':0,'warmup_signal_count':0,'signal_count':len(events),'tickers':symbols,'signal_events':[bt._serialize_signal_event(e) for e in events],'trade_events':state.trade_events,'trade_count':sum(e['event_type'] in {'open','add','close'} for e in state.trade_events),'overall_pnl_history':state.overall_pnl_history,'ticker_pnl_history':state.ticker_pnl_history,'daily_max_open_exposure':bt._daily_exposure_rows(state.daily_max_exposure),'anchor_bars':anchor,'indicator_series':indicators,'open_positions':open_positions,'realized_pnl':sum(state.realized_by_ticker.values()),'unrealized_pnl':sum(p['unrealized_pnl'] for p in open_positions),'entry_diagnostics':diagnostics,'liquidate_before_market_close':False,'run_exit_strategy':True}
