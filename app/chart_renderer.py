"""Independent chart layers on a shared session-time axis; no execution logic."""
from zoneinfo import ZoneInfo
import math
import pandas as pd
import numpy as np
from matplotlib import dates as md
from matplotlib.patches import Rectangle,Patch
from matplotlib.lines import Line2D


def x(value):return md.date2num(pd.Timestamp(value).to_pydatetime())
def finite(value):return value is not None and math.isfinite(value)


def draw_chart(ax,ticker,data,entries,exits,signals,entries_and_exits,lux_algo_symbols,indicators,settings,reversals):
	from chart_time_axis import ChartTimeAxis
	x=ChartTimeAxis(data.get('sessions',[]))
	o=data['options'];layers=data['layers'];tf=o['plotted_candlesticks'];candles=layers.get(tf,{}).get('candles',[])
	duration=pd.Timedelta(tf).total_seconds()/86400;handles=[];zone_labels=set()
	for row in candles:
		when=x(row['time']);op,cl,hi,lo=(row[k] for k in ('open','close','high','low'))
		color='green' if cl>=op else 'red'
		ax.vlines(when,lo,hi,color=color,linewidth=1,zorder=2)
		ax.add_patch(Rectangle((when-duration*.325,min(op,cl)),duration*.65,abs(cl-op) or max(hi-lo,.01)*.02,facecolor=color,edgecolor=color,alpha=.8,zorder=2))
	last=x(data['end'])
	if indicators:
		for selector,names,color,width in [('plotted_fast_ma',('fast_moving_average',),'#1F77B4',1.3),('plotted_slow_ma',('slow_moving_average',),'#FF8C00',1.8),('plotted_bollingers',('bollinger_upper','bollinger_lower'),'#FF69B4',1.3)]:
			selected=o[selector];rows=layers.get(selected,{}).get('indicators',[])
			for name in names:
				values=[r[name] if finite(r.get(name)) else np.nan for r in rows]
				if rows and any(math.isfinite(v) for v in values):
					timestamps=[x(r['completed_at']) for r in rows]
					if timestamps[-1]<x(data['end']):timestamps.append(x(data['end']));values.append(values[-1])
					line,=ax.plot(timestamps,values,color=color,linewidth=width,label=name+' ('+selected+')',zorder=3);handles.append(line)
		# Keep the optional DEMA5 exit diagnostic on its original 4h layer.
		if settings.get('cross_exit_method')=='dema5':
			rows=layers.get('4h',{}).get('indicators',[])
			if rows:
				line,=ax.plot([x(r['completed_at']) for r in rows],[r.get('dema5') if finite(r.get('dema5')) else np.nan for r in rows],color='#9467BD',linewidth=1.2,label='dema5 (4h)');handles.append(line)
		for selector in ('plotted_range_zone','plotted_stretch_zones'):
			selected=o[selector];span=pd.Timedelta(selected).total_seconds()/86400
			for r in layers.get(selected,{}).get('indicators',[]):
				upper,lower=r.get('bollinger_upper'),r.get('bollinger_lower')
				if not all(finite(v) for v in (upper,lower)) or upper<=lower:continue
				completed=x(r['completed_at']);left=max(completed,x(data['start']));right=min(x(pd.Timestamp(r['completed_at'])+pd.Timedelta(selected)),x(data['end']))
				if right<=left:continue
				areas=[];mid=(upper+lower)/2
				if selector=='plotted_range_zone':
					if r.get('ranging_market') and settings.get('shade_ranging_area',True):areas.append(('Range',lower,upper,'#D3E85A'))
					if settings.get('range_transition_zones_enabled',True):
						if r.get('bullish_transition'):areas.append(('Range likely transitioning to a bullish stretch',lower,upper,'#FFFF00'))
						if r.get('bearish_transition'):areas.append(('Range likely transitioning to a bearish stretch',lower,upper,'#FFFF00'))
				else:
					if r.get('extreme_bullish_price_stretch'):areas.append(('Bullish price stretch',mid,max(upper,r.get('high') or upper),'#BC95E0'))
					if r.get('extreme_bearish_price_stretch'):areas.append(('Bearish price stretch',min(lower,r.get('low') or lower),mid,'#F4AF69'))
				for label,bottom,top,color in areas:
					ax.add_patch(Rectangle((left,bottom),right-left,top-bottom,facecolor=color,alpha=.45,edgecolor='none',zorder=.3))
					key=(label,selected,color)
					if key not in zone_labels:handles.append(Patch(facecolor=color,alpha=.45,label=label+' ('+selected+')'));zone_labels.add(key)
		selected=o['plotted_ichimoku_cloud'];cloud=layers.get(selected,{}).get('ichimoku',[])
		if cloud:
			cx=np.array([x(r['time']) for r in cloud]);a=np.array([r['span_a'] for r in cloud]);b=np.array([r['span_b'] for r in cloud])
			ax.fill_between(cx,a,b,where=a>b,interpolate=True,color='#B8E0B8',alpha=.45,zorder=.1)
			ax.fill_between(cx,a,b,where=a<b,interpolate=True,color='#F2B8B8',alpha=.45,zorder=.1)
			handles.extend([Patch(facecolor='#B8E0B8',alpha=.45,label='Ichimoku A > B ('+selected+')'),Patch(facecolor='#F2B8B8',alpha=.45,label='Ichimoku A < B ('+selected+')')]);last=max(last,float(cx[-1]))
	def fallback_price(when):
		available=[r for r in layers.get(tf,{}).get('indicators',[]) if finite(r.get('close')) and pd.Timestamp(r['completed_at'])<=pd.Timestamp(when)]
		if available:return available[-1]['close']
		available=[r for r in candles if pd.Timestamp(r['completed_at'])<=pd.Timestamp(when)]
		return available[-1]['raw_close'] if available else None
	if entries_and_exits:
		for event in entries:
			long=event['side']=='long';owner=event.get('strategy_type','cross')
			marker='D' if owner=='post_stretch_range' else ('2' if long else '1') if owner=='range' else ('^' if long else 'v')
			color='#7B2CBF' if long else '#00A6A6';when=x(event['time']);price=float(event['price'])
			ax.scatter(when,price,marker=marker,s=180 if owner=='range' else 90,color=color,edgecolors=None if owner=='range' else 'black',linewidths=2.6 if owner=='range' else .5,zorder=5)
			ax.annotate(f"${abs(price*float(event['num_shares'])):,.2f}",(when,price),xytext=(0,10),textcoords='offset points',ha='center',fontsize=7)
		for event in exits or []:
			price=event.get('price') or fallback_price(event['time'])
			if price is None:continue
			owner=event.get('strategy_type','cross');when=x(event['time'])
			ax.scatter(when,price,marker='P' if owner=='post_stretch_range' else '*' if owner=='range' else 's',s=150 if owner=='range' else 70,facecolors='#FF00CC',edgecolors='#FF00CC',linewidths=1.2,zorder=5)
			ax.annotate('EXIT',(when,price),xytext=(0,9),textcoords='offset points',ha='center',fontsize=7)
		for label,marker,color in [('Cross long entry','^','#7B2CBF'),('Cross short entry','v','#00A6A6'),('Cross exit','s','#FF00CC'),('Range long entry','2','#7B2CBF'),('Range short entry','1','#00A6A6'),('Range exit','*','#FF00CC'),('Post-stretch range long entry','D','#7B2CBF'),('Post-stretch range short entry','D','#00A6A6'),('Post-stretch range exit','P','#FF00CC')]:handles.append(Line2D([0],[0],marker=marker,linestyle='None',color=color,label=label))
	if lux_algo_symbols:
		for event in signals or []:
			price=event.get('price')
			if price is None:price=fallback_price(event['time'])
			if price is None:continue
			side=str(event.get('side') or '').lower();name=str(event.get('signal') or side or 'signal');exit_signal=name in {'bullish_exit','bearish_exit'}
			marker='X' if exit_signal else '^' if side=='buy' else 'v' if side=='sell' else 'x'
			color=('#0080FF' if name=='bullish_exit' else '#FF8C00') if exit_signal else 'green' if side=='buy' else 'red' if side=='sell' else 'gray'
			when=x(event['time']);ax.scatter(when,float(price),marker=marker,s=28,color=color,alpha=1 if exit_signal else .45,zorder=3)
			ax.annotate('TV '+str(event.get('timeframe') or '')+' '+name,(when,float(price)),xytext=(3,3),textcoords='offset points',fontsize=6,alpha=.65)
		for label,marker,color in [('TradingView buy signal (TV)','^','green'),('TradingView sell signal (TV)','v','red'),('TradingView bullish exit (TV)','X','#0080FF'),('TradingView bearish exit (TV)','X','#FF8C00')]:handles.append(Line2D([0],[0],marker=marker,linestyle='None',color=color,label=label))
	if indicators and settings.get('fast_moving_average_reversed',True):
		for r in reversals or []:
			minimum=r.get('turning_point_type')=='minimum'
			ax.scatter(x(r['time']),r['value'],marker='o',s=46,facecolors='black' if minimum else 'white',edgecolors='black',linewidths=1.2,zorder=7)
		if reversals:
			for minimum in [True,False]:handles.append(Line2D([0],[0],marker='o',linestyle='None',markerfacecolor='black' if minimum else 'white',markeredgecolor='black',label=('Minimum' if minimum else 'Maximum')+' fast MA reversal ('+settings.get('fast_moving_average_reversal_timeframe','4h')+')'))
	x.configure(ax)
	ax.set_xlim(x(data['start'])-duration,last+duration*.5)
	ax.set_title(ticker+' — '+tf+(' Heikin-Ashi' if o['candlestick_type']=='heikin_ashi' else ' regular')+' candles')
	ax.set_ylabel('Price ($)');ax.set_xlabel('Time (America/New_York)');ax.grid(True,alpha=.25)
	if handles:
		from chart_legend import place_legend_sections
		place_legend_sections(ax,handles)
