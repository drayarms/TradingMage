"""Small grouped chart keys placed in the least occupied parts of the axes."""
import numpy as np
from matplotlib.legend import Legend


def _group(label):
	if label.startswith('TradingView'):return 'LuxAlgo'
	if 'reversal' in label:return 'Fast MA turning points'
	if label.startswith(('Cross ','Post-stretch ')) or label.startswith('Range ') and label.endswith(('entry','exit')):return 'Trades'
	if label.startswith(('Range','Bullish price stretch','Bearish price stretch')):return 'Zones'
	return 'Indicators'


def _label(label):
	for old,new in [('fast_moving_average','Fast MA'),('slow_moving_average','Slow MA'),('bollinger_upper','BB upper'),('bollinger_lower','BB lower'),('Ichimoku','Cloud'),('Range likely transitioning to a bullish stretch','Range → bullish stretch'),('Range likely transitioning to a bearish stretch','Range → bearish stretch'),('Post-stretch range','Post-stretch'),('TradingView ','')]:label=label.replace(old,new)
	return label.replace(' signal (TV)','').replace(' (TV)','').replace(' entry','').replace(' fast MA reversal','')


def place_legend_sections(ax,handles):
	groups={}
	for handle in handles:
		label=handle.get_label();groups.setdefault(_group(label),[]).append((handle,_label(label)))
	if not groups:return []
	canvas=ax.figure.canvas;canvas.draw();renderer=canvas.get_renderer();bounds=ax.get_window_extent(renderer)
	try:
		image=np.asarray(canvas.buffer_rgba());height=image.shape[0]
		crop=image[max(0,int(height-bounds.y1)):min(height,int(height-bounds.y0)),max(0,int(bounds.x0)):int(bounds.x1),:3].astype(float)/255
		ink=(1-crop.min(axis=2))**2
	except AttributeError:ink=np.zeros((max(1,int(bounds.height)),max(1,int(bounds.width))))
	integral=np.pad(ink.cumsum(0).cumsum(1),((1,0),(1,0)));h,w=ink.shape
	sections=[]
	for title,items in groups.items():
		legend=Legend(ax,[item[0] for item in items],[item[1] for item in items],title=title,fontsize=5.5,title_fontsize=6.,loc='lower left',framealpha=.94,facecolor='white',edgecolor='#BBBBBB',borderaxespad=0,borderpad=.3,labelspacing=.18,handlelength=1.1,handletextpad=.35,markerscale=.65)
		box=legend.get_window_extent(renderer);sections.append((box.width*box.height,legend,box.width/bounds.width,box.height/bounds.height))
	reserved=[];legends=[];margin=.012
	for _,legend,width,height in sorted(sections,key=lambda item:-item[0]):
		width=min(width,.97);height=min(height,.97)
		max_x=max(margin,1-width-margin);max_y=max(margin,1-height-margin)
		candidates=[(max_x,max_y),(margin,max_y),(max_x,margin),(margin,margin)]
		candidates.extend((xx,yy) for yy in np.linspace(max_y,margin,15) for xx in np.linspace(max_x,margin,19))
		best=None
		for left,bottom in candidates:
			right=left+width;top=bottom+height;overlap=0.
			for l,b,r,t in reserved:overlap+=max(0,min(right+.008,r)-max(left-.008,l))*max(0,min(top+.008,t)-max(bottom-.008,b))
			x0=max(0,int(left*w));x1=min(w,int(right*w));y0=max(0,int((1-top)*h));y1=min(h,int((1-bottom)*h))
			amount=integral[y1,x1]-integral[y0,x1]-integral[y1,x0]+integral[y0,x0]
			score=float(amount)+overlap*w*h*1000
			if best is None or score<best[0]:best=(score,left,bottom)
		_,left,bottom=best;legend.set_bbox_to_anchor((left,bottom),transform=ax.transAxes);ax.add_artist(legend)
		reserved.append((left,bottom,left+width,bottom+height));legends.append(legend)
	return legends
