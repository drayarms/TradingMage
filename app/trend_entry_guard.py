"""Cycle-scoped adverse entry attempts; independent of inventory cost averaging."""
import math
import pandas as pd


def finite(value):return value is not None and math.isfinite(value)


def attach_trend_cycles(frame):
	out=frame.copy();previous={'long':False,'short':False};current={'long':None,'short':None};cycles={'long':[],'short':[]}
	for row in frame.to_dict('records'):
		active={'long':bool(row.get('universal_trough',False)),'short':bool(row.get('universal_peak',False))}
		starts=[side for side in active if active[side] and not previous[side]]
		if starts:
			current=dict.fromkeys(current)
			if len(starts)==1:current[starts[0]]=pd.Timestamp(row['completed_at']).isoformat()
		for side in current:cycles[side].append(current[side])
		previous=active
	for side,values in cycles.items():out['trend_'+side+'_cycle_id']=pd.Series(values,index=out.index,dtype=object)
	return out


def restore_live_cycle_ids(frame,saved):
	"""A rolling history may reconstruct a continuing zone with a later start."""
	out=frame.copy()
	if out.empty:return out
	for owner,state in saved.items():
		field=owner+'_cycle_id';current=out.iloc[-1].get(field)
		if current is not None and pd.notna(current) and state.get('cycle_id') and state.get('observed_at') and pd.Timestamp(current)<=pd.Timestamp(state['observed_at']):out.loc[out.index[-1],field]=state['cycle_id']
	return out


class PotentialUnprofitableTrendGuard:
	def __init__(self):self.states={}
	def state(self,owner,ticker,row):
		identity=row.get(owner+'_cycle_id')
		if identity is not None and pd.isna(identity):identity=None
		key=(owner,ticker);state=self.states.get(key)
		if state is None or state['cycle_id']!=identity:
			state=dict(cycle_id=identity,reference_price=None,attempt_count=0,adverse_reversals=[],liquidated_reversals=[]);self.states[key]=state
		if row.get('completed_at') is not None:state['observed_at']=pd.Timestamp(row['completed_at']).isoformat()
		return state
	def filled(self,owner,ticker,row,price):
		state=self.state(owner,ticker,row)
		if state['cycle_id'] is not None and finite(price) and price>0:state['reference_price']=float(price)
	def evaluate(self,owner,ticker,row,price,reversal,settings):
		state=self.state(owner,ticker,row)
		result=dict(block=False,liquidate=False,new_attempt=False,state=state)
		if not (settings.block_potential_unprofitable_trend_entry or settings.exit_potential_unprofitable_trend_entry) or row.get('ranging_market',False) or state['cycle_id'] is None:return result
		if reversal in state['liquidated_reversals']:
			result['block']=True;return result # Skip the triggering dip/lift, not the whole cycle.
		reference=state['reference_price'];atr=row.get('atr')
		if not all(finite(v) for v in (price,reference,atr)) or atr<=0:return result
		delta=(reference-price) if owner=='trend_long' else (price-reference)
		threshold=settings.potential_unprofitable_trend_entry_atr_multiple*atr
		if delta<threshold:return result
		if reversal not in state['adverse_reversals']:
			state['adverse_reversals'].append(reversal);state['attempt_count']+=1;result['new_attempt']=True
		result['block']=settings.block_potential_unprofitable_trend_entry
		if settings.exit_potential_unprofitable_trend_entry and state['attempt_count']>=settings.potential_unprofitable_trend_entry_exit_attempts:
			state['liquidated_reversals'].append(reversal);result.update(block=True,liquidate=True)
		return result
