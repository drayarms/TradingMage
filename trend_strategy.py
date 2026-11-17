"""Pure trend entry/exit decisions shared by simulation and live virtual books."""
import math
import pandas as pd
from trend_plot_zones import annotate_trend_zones
from universal_plot_zones import annotate_universal_zones
from chart_data import heikin_ashi

def finite(*values):return all(v is not None and math.isfinite(v) for v in values)

def prepare_strategy_frame(frame,points,options):
	frame=annotate_trend_zones(frame,points,trend_cloud_condition_enabled=options.trend_cloud_condition_enabled,trend_heikin_ashi_streak_condition_enabled=options.trend_heikin_ashi_streak_condition_enabled,trend_cloud_fast_ma_band_gap_fraction=options.trend_cloud_fast_ma_band_gap_fraction,trend_heikin_ashi_streak_min_candles=options.trend_heikin_ashi_streak_min_candles,trend_heikin_ashi_streak_max_ma_gap_atr_multiple=options.trend_heikin_ashi_streak_max_ma_gap_atr_multiple)
	frame=annotate_universal_zones(frame,options.universal_zone_gradient_candles,points,universal_zone_min_fast_ma_change_atr_multiple=options.universal_zone_min_fast_ma_change_atr_multiple)
	for field in ('universal_peak','universal_trough'):
		starts=frame[field] & ~frame[field].shift(1,fill_value=False)
		last=None;elapsed=[]
		for i,active in enumerate(frame[field]):
			if active:last=i
			elapsed.append(i-last if last is not None else None)
		frame[field+'_candles_since_last_zone']=pd.Series(elapsed,index=frame.index,dtype=float)
		frame[field+'_zone_id']=pd.Series([pd.Timestamp(t).isoformat() if on else None for t,on in zip(frame.completed_at,starts)],index=frame.index).ffill().where(frame[field],None)
	return attach_entry_exit_context(frame,points)

def attach_entry_exit_context(frame,points):
	"""Native completed-candle reversal windows and causally confirmed gap history."""
	frame=frame.copy()
	ha=heikin_ashi(frame) if all(k in frame for k in ('open','high','low','close')) else pd.DataFrame(index=frame.index,columns=['open','close'],dtype=float)
	colors=(ha.close>ha.open).astype(int)-(ha.close<ha.open).astype(int)
	last_zone=None;setups={'long':[],'short':[]}
	for i,row in enumerate(frame.to_dict('records')):
		if row.get('universal_peak',False):last_zone='peak'
		if row.get('universal_trough',False):last_zone='trough'
		for side,direction,zone in (('long',1,'trough'),('short',-1,'peak')):
			reversal=i>0 and colors.iloc[i]==direction and colors.iloc[i-1]==-direction
			setups[side].append(pd.Timestamp(row['completed_at']).isoformat() if reversal and last_zone==zone else None)
	for side in setups:frame['trend_'+side+'_reversal_id']=setups[side]
	for label in ('upper','lower'):
		band=frame['bollinger_'+label]
		frame['trend_projected_'+label+'_bollinger']=band+(band-band.shift(2))/2
	gaps=frame.bollinger_upper-frame.bollinger_lower;events=[]
	for point in points:
		confirmed=pd.Timestamp(point['confirmed_at']);evaluation=pd.Timestamp(point.get('time',confirmed-pd.Timedelta(hours=4)))
		if evaluation in frame.index:
			i=frame.index.get_loc(evaluation)
			if pd.Timestamp(frame.iloc[i].completed_at)<=confirmed:events.append((confirmed.value,i))
	events=sorted(set(events));cursor=0;known=[];widest=[]
	for i,t in enumerate(frame.completed_at):
		while cursor<len(events) and events[cursor][0]<=pd.Timestamp(t).value:
			if events[cursor][1] not in known:known.append(events[cursor][1])
			cursor+=1
		values=gaps.iloc[known[-2]:i+1] if len(known)>=2 else pd.Series(dtype=float)
		values=values[values.map(lambda x:finite(x) and x>0)]
		widest.append(float(values.max()) if len(values) else float('nan'))
	frame['trend_widest_gap_since_penultimate_turn']=widest
	return frame

def entry_reason(side,price,row,position_open,last_reversal=None,cooldown_candles=2):
	if not row or not finite(price,row.get('fast_moving_average')):return None
	other='short' if side=='long' else 'long'
	if row.get('strong_'+other+'_trend',False):return None
	opposite='universal_peak' if side=='long' else 'universal_trough'
	elapsed=row.get(opposite+'_candles_since_last_zone')
	if cooldown_candles>0 and finite(elapsed) and 0<=elapsed<=cooldown_candles:return None
	identity=row.get('trend_'+side+'_reversal_id')
	if identity is None or pd.isna(identity) or identity==last_reversal:return None
	band=row.get('bollinger_upper' if side=='long' else 'bollinger_lower')
	if finite(band) and abs(price-row['fast_moving_average'])<abs(price-band):
		return ('trend_dip_entry' if side=='long' else 'trend_lift_entry',identity)

def observe_band_visit(side,price,row,state):
	"""Compare first observed minute prices on distinct favorable-band visits."""
	band=row.get('bollinger_upper' if side=='long' else 'bollinger_lower');atr=row.get('atr')
	if not finite(price,band,atr) or atr<=0:return
	near=price>band-state['proximity']*atr if side=='long' else price<band+state['proximity']*atr
	if near and not state.get('in_visit',False):
		previous=state.get('last_visit_price')
		state['failed_visit']=finite(previous) and (price<previous if side=='long' else price>previous)
		state['last_visit_price']=price
	elif not near:state['failed_visit']=False
	state['in_visit']=bool(near)

def exit_reason(side,price,row,multiple,contraction_divisor=2.,proximity_atr_multiple=.2,visit=None):
	if not row:return None
	band=row.get('bollinger_upper' if side=='long' else 'bollinger_lower');atr=row.get('atr')
	if finite(price,band,atr) and atr>0 and ((price>band+multiple*atr) if side=='long' else (price<band-multiple*atr)):return 'trend_bollinger_atr_exit'
	zone='universal_peak' if side=='long' else 'universal_trough'
	if row.get(zone,False) and not row.get('strong_'+side+'_trend',False):return zone
	if visit and visit.get('failed_visit'):return 'trend_worse_bollinger_visit'
	upper,lower=row.get('bollinger_upper'),row.get('bollinger_lower');widest=row.get('trend_widest_gap_since_penultimate_turn')
	if not finite(price,upper,lower,atr,widest) or atr<=0 or upper<=lower or widest<=0:return None
	if upper-lower>=widest/contraction_divisor:return None
	opposite='lower' if side=='long' else 'upper'
	for value in (row.get('bollinger_'+opposite),row.get('trend_projected_'+opposite+'_bollinger')):
		if finite(value) and value>0 and (price<value+proximity_atr_multiple*atr if side=='long' else price>value-proximity_atr_multiple*atr):return 'trend_contracted_opposing_bollinger_exit'
