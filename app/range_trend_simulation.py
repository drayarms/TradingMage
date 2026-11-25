"""Range/trend simulation with independent strategy books and causal prices."""
import math,os
from itertools import groupby
import pandas as pd
from backtest_progress import BacktestProgress
from backtest_sessions import BacktestSessions
from golden_cross_history import GoldenCrossHistory
from cross_strategy_indicators import fast_moving_average_reversed,REVERSAL_TIMEFRAMES
from range_strategy import attach_turning_point_gap_reference,attach_post_stretch_context,GAP_REFERENCE
from market_filters import MARKET_FIELDS
from range_trend_settings import STRATEGY_NAME
from range_trend_portfolio import RangeTrendPortfolio
from trend_strategy import prepare_strategy_frame

def run_simulation(bt, api, start, end, tickers, position_size, settings,plot_options=None,prepare_plot_data=True):
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
	progress.stage('Loading trading calendar...')
	calendar=BacktestSessions.load(api,start_dt,end_dt,fallback_apis=getattr(bt,'calendar_apis',()))
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
	# Zone reporting also needs confirmed points when both trading books are disabled.
	points={ticker:fast_moving_average_reversed(frame,'4h') for ticker,frame in frames.items()}
	from cross_entry_policy import attach_cross_turning_context, attach_cross_entry_context, ENTRY_CONTEXT_FIELDS
	frames={ticker:attach_cross_turning_context(attach_turning_point_gap_reference(attach_post_stretch_context(frame if all(k in frame for k in ENTRY_CONTEXT_FIELDS) else attach_cross_entry_context(frame)),points.get(ticker,[])),points.get(ticker,[])) for ticker,frame in frames.items()}
	state=SimState()
	state.market_data={f'{c}_1m':bt.trade_records_instance.dataframe_column_to_dict(prices,c) for c in ('open','high','low','close','volume')}
	state.market_data.update(anchor_atr={},anchor_ohlc={},market_close_liquidation_times=[],execution_sessions=calendar)
	seen=set();diagnostics=[]
	frames={ticker:prepare_strategy_frame(frame,points.get(ticker,[]),plot_options) for ticker,frame in frames.items()}
	portfolio=RangeTrendPortfolio(bt,settings,state,frames,base)
	from heikin_ashi_filter import load_hourly_runs
	progress.stage("Loading completed 1h Heikin-Ashi filters...")
	if any(portfolio.active(owner) for owner in portfolio.books):
		portfolio.heikin_ashi_runs=load_hourly_runs(api,settings,symbols,start_dt,end_dt,frames)
		portfolio.set_hourly_trends(portfolio.heikin_ashi_runs,plot_options)
	reports=portfolio.direction_reports
	def record(when):
		portfolio.record(when)
	bt.recording_enabled=True;bt.diagnostic_logging_enabled=False
	timeline=bt._build_backtest_timeline(state,events,start_dt,end_dt,market_bar_interval_minutes=1)
	for ticker,(times,_) in portfolio.heikin_ashi_runs.index.items():
		for stamp in times:
			when=pd.Timestamp(int(stamp),tz='UTC')
			if pd.Timestamp(start_dt)<=when<=pd.Timestamp(end_dt):timeline.append(dict(kind='heikin_ashi_completion',dt=when.to_pydatetime(),payload=dict(ticker=ticker)))
	context_times={ticker:pd.DatetimeIndex(frame.completed_at).asi8 for ticker,frame in frames.items() if not frame.empty}
	# Always evaluate exits on completed 4h boundaries, even without a minute bar.
	for ticker,frame in frames.items():
		for when in frame.completed_at:
			if pd.Timestamp(start_dt)<=when<=pd.Timestamp(end_dt):timeline.append(dict(kind='anchor_completion',dt=when.to_pydatetime(),payload=dict(ticker=ticker)))
	timeline.sort(key=lambda item:(item['dt'], {'market_bar':0, 'death_cross':1, 'golden_cross':1, 'dema5_death_cross':1, 'dema5_golden_cross':1, 'signal':2,'confirmation_candle':3}.get(item['kind'],4)))
	for when,items in groupby(timeline,key=lambda item:item['dt']):
		progress.update(when)
		batch=list(items)
		tickers_at_time=sorted({item['payload']['ticker'] for item in batch})
		before_trades=len(portfolio.aggregate.trade_events)
		for item in batch:
			if item['kind']=='market_bar':
				payload=item['payload'];state.last_price_by_ticker[payload['ticker']]=float(payload['close'])
		portfolio.track_exposure(when)
		market_tickers={item['payload']['ticker'] for item in batch if item['kind']=='market_bar'}
		for ticker in tickers_at_time:portfolio.process_heikin_ashi_exits(ticker,when)
		for ticker in tickers_at_time:portfolio.process_zone_exits(ticker,when,record_zone=ticker in market_tickers)
		portfolio.process_exit_intents(when)
		for ticker in tickers_at_time: portfolio.process_range(ticker,when,entries=False)
		for ticker in tickers_at_time:portfolio.process_trend(ticker,when,entries=False)
		# Exits have priority; pending orders are revalidated against current context.
		portfolio.limit_orders.process(when,batch)
		order=('trend','range') if settings.entry_strategy_precedence=='trend' else ('range','trend')
		for family in order:
			for ticker in sorted(market_tickers):
				if family=='trend':portfolio.process_trend(ticker,when,entries=True)
				else:portfolio.process_range(ticker,when,entries=True)
		portfolio.track_exposure(when)
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
		indicators[ticker]=[{'time':t.isoformat(),'completed_at':row.completed_at.isoformat(),**{k:(None if pd.isna(row.get(k)) else bool(row[k]) if k in MARKET_FIELDS[1:4] else float(row[k])) for k in ('fast_moving_average','slow_moving_average','bollinger_upper','bollinger_lower','dema5','atr','high','low',GAP_REFERENCE,*MARKET_FIELDS)}} for t,row in visible.iterrows()]
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
		report['trade_count']=sum((e.get('strategy_direction')==owner if owner.startswith('trend_') else e.get('strategy_type')==owner) and e.get('event_type') in {'open','add','close'} for e in state.trade_events)
	for direction, report in reports.items():
		report['trade_count'] = sum(e.get('strategy_direction') == direction and e.get('event_type') in {'open', 'add', 'close'} for e in state.trade_events)
	chart_data={}
	if prepare_plot_data and plot_options is not None:
		from chart_data import prepare_chart_data
		progress.stage('Calculating selected chart timeframes...')
		chart_data=prepare_chart_data(api,settings,frames,start_dt,end_dt,plot_options,hourly_frames=getattr(portfolio.heikin_ashi_runs,'raw_frames',None))
	return {'max_open_positions_report':portfolio.exposure.to_text(),'max_open_positions_exposure':portfolio.exposure.to_dict(),'chart_data':chart_data,'range_transition_zones':portfolio.transition_zone_intervals,'execution_policy':dict(calendar='Alpaca',timezone='America/New_York',session='04:00-20:00 capped by calendar session bounds',overnight=False,extended_fill_model='DAY limit; subsequent full 1m bar touch, positive volume sufficient for full quantity; no partial fills'), 'execution_order_events':portfolio.limit_orders.events,'gradient_blocked_exit_intents':[dict(ticker=k[1],strategy_type=k[0],signal_time=e.get('time'),reason=e.get('exit_reason')) for k,(e,p) in portfolio.blocked_exit_intents.items()], 'pending_exit_intents':[dict(ticker=k[1],strategy_type=k[0],signal_time=e.get('time'),reason=e.get('exit_reason')) for k,(e,p) in portfolio.limit_orders.closed_exit_intents.items()], 'pending_execution_orders':[dict(ticker=k[1],strategy_type=k[0],limit_price=o['limit'],submitted_at=o['submitted'].isoformat(),action='exit' if o['exit_order'] else 'entry') for k,o in portfolio.limit_orders.pending.items()],'strategy_results':portfolio.strategy_reports,'virtual_trade_events':portfolio.virtual_trade_events,'virtual_open_positions':portfolio.open_positions(virtual=True),'net_positions':portfolio.net_positions,'trend_entry_guard_events':portfolio.trend_entry_guard_events,'trend_entry_guard_state':[dict(ticker=t,strategy_direction=o,**s) for (o,t),s in portfolio.trend_entry_guard.states.items()], 'fast_moving_average_reversed':reversals, 'strategy_mode':settings.strategy_mode, 'direction_results':reports, 'strategy_name':STRATEGY_NAME,'exit_strategy':'range_and_trend_independent','anchor_timeframe':'4h','ATR_period':settings.atr_period,'ATR_multiplier':None,'start':start_dt.isoformat(),'end':end_dt.isoformat(),'position_size':base,'strategy_settings':settings.to_dict(),'warmup_sessions':0,'warmup_signal_count':0,'signal_count':len(events),'tickers':symbols,'signal_events':[bt._serialize_signal_event(e) for e in events],'trade_events':state.trade_events,'trade_count':sum(e['event_type'] in {'open','add','close'} for e in state.trade_events),'overall_pnl_history':state.overall_pnl_history,'ticker_pnl_history':state.ticker_pnl_history,'capital_exposure_basis':'net_positions','daily_max_open_exposure':portfolio.exposure.daily_rows(),'daily_max_open_exposure_summary':portfolio.exposure.summary(),'anchor_bars':anchor,'indicator_series':indicators,'open_positions':open_positions,'realized_pnl':sum(state.realized_by_ticker.values()),'unrealized_pnl':sum(p['unrealized_pnl'] for p in open_positions),'entry_diagnostics':diagnostics,'liquidate_before_market_close':False,'run_exit_strategy':True}
