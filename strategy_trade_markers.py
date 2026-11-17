"""One color per action; one shape per strategy and position side."""
ENTRY_COLOR='#16BDB4'
EXIT_COLOR='#E83E8C'

def position_side(event):
	return event.get('position_side') or ('long' if str(event.get('side','long')) in {'long','buy','sell'} else 'short')

def trade_marker(event):
	owner=event.get('strategy_type','trend');long=position_side(event)=='long'
	return ('d' if long else 's') if owner=='post_stretch_range' else ('2' if long else '1') if owner=='range' else ('^' if long else 'v')

def legend_styles():
	for owner,label in (('trend','Trend'),('range','Range'),('post_stretch_range','Range stretch')):
		for side in ('long','short'):
			for action,color in (('entry',ENTRY_COLOR),('exit',EXIT_COLOR)):
				yield label+' '+side+' '+action,trade_marker(dict(strategy_type=owner,position_side=side)),color
