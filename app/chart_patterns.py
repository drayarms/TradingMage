"""Short separated dash hatches for display overlays."""
import numpy as np
from matplotlib.hatch import HatchPatternBase
from matplotlib.path import Path

class ShortDashHatch(HatchPatternBase):
	def __init__(self,hatch,density):
		self.rows=int(hatch.count('-')*density);self.columns=8
		self.num_vertices=self.rows*self.columns*2
	def set_vertices_and_codes(self,vertices,codes):
		k=0
		for y in (np.arange(self.rows)+.5)/self.rows if self.rows else []:
			for x in (np.arange(self.columns)+.5)/self.columns:
				vertices[k:k+2]=[(x-.15/self.columns,y),(x+.15/self.columns,y)]
				codes[k:k+2]=[Path.MOVETO,Path.LINETO];k+=2

def install_short_dashes():
	from matplotlib import hatch
	if ShortDashHatch not in hatch._hatch_types:
		# Preserve '+' horizontal strokes while replacing only the '-' pattern.
		original=hatch.HorizontalHatch
		class PlusHorizontalHatch(original):
			def __init__(self,pattern,density):super().__init__(pattern.replace('-',''),density)
		hatch._hatch_types=[PlusHorizontalHatch if c is original else c for c in hatch._hatch_types]+[ShortDashHatch]
		Path.hatch.cache_clear()
