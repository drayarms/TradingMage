"""Peak gross actual open-position market values at observed simulation times."""
import copy

class OpenExposureTracker:
	def __init__(self,tickers=()):
		self.by_ticker={t:dict(max_open_market_value=0.,time=None) for t in tickers}
		self.overall=dict(max_open_market_value=0.,time=None,by_ticker_at_peak={})

	def update(self,when,market_values):
		when=when.isoformat() if hasattr(when,'isoformat') else str(when)
		for ticker,value in market_values.items():
			peak=self.by_ticker.setdefault(ticker,dict(max_open_market_value=0.,time=None))
			if value>peak['max_open_market_value']: peak.update(max_open_market_value=float(value),time=when)
		total=float(sum(market_values.values()))
		if total>self.overall['max_open_market_value']:
			self.overall.update(max_open_market_value=total,time=when,by_ticker_at_peak={t:float(v) for t,v in market_values.items() if v>0})

	def to_dict(self):
		return copy.deepcopy(dict(overall=self.overall,by_ticker=self.by_ticker))

	def to_text(self):
		lines=['MAXIMUM OPEN-POSITION MARKET VALUE','USD; gross market value of actual long and short holdings. Virtual positions excluded.',
			'Observed at completed 1m updates and signal arrivals, before exits and after entries.',
			'Cross and range quantities are combined per stock. Times include UTC offsets.',
			'','Stock       Peak open market value   First observed at','-'*78]
		for ticker,row in sorted(self.by_ticker.items(),key=lambda item:(-item[1]['max_open_market_value'],item[0])):
			lines.append(f"{ticker:<10} ${row['max_open_market_value']:>21,.2f}   {row['time'] or '--'}")
		lines.extend(['',f"Overall simultaneous peak: ${self.overall['max_open_market_value']:,.2f}",f"First observed at: {self.overall['time'] or '--'}",'Holdings at the overall peak:'])
		for ticker,value in sorted(self.overall['by_ticker_at_peak'].items()): lines.append(f'  {ticker:<10} ${value:,.2f}')
		lines.extend(['','The overall peak is measured at one moment; individual-stock peaks can occur at different times.',
			'These are observed market values, before broker margin requirements; intraminute tick highs are not measured.'])
		return '\n'.join(lines)+'\n'
