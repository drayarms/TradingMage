"""Side-by-side net inventory capital and enabled virtual-ledger gross exposure."""
import copy
import math
import statistics
from open_exposure_report import OpenExposureTracker


def position_exposures(books,enabled,prices):
	"""Use a common current mark per ticker before offsetting strategy shares."""
	positions={}
	for owner,book in books.items():
		if not enabled.get(owner,False):continue
		for ticker,position in book.positions.items():positions.setdefault(ticker,[]).append(position)
	gross={};net={};shares={};long_value=short_value=0.
	for ticker,held in positions.items():
		price=float(prices.get(ticker,held[0].avg_price_per_share))
		quantity=math.fsum(p.num_shares*(1 if p.side=='long' else -1) for p in held)
		shares[ticker]=quantity
		gross[ticker]=math.fsum(abs(p.num_shares)*price for p in held)
		net[ticker]=abs(quantity)*price
		if quantity>0:long_value+=net[ticker]
		elif quantity<0:short_value+=net[ticker]
	return dict(net_positions=shares,net_by_ticker=net,virtual_ledger_by_ticker=gross,net_open_exposure=math.fsum(net.values()),gross_open_exposure=math.fsum(gross.values()),net_long_open_exposure=long_value,net_short_open_exposure=short_value)


def exposure_summary(daily):
	values=[value for value in daily.values() if value>0]
	return dict(days_observed=len(daily),days_with_exposure=len(values),mean=statistics.mean(values) if values else 0.,standard_deviation=statistics.stdev(values) if len(values)>1 else 0.,max=max(values) if values else 0.,min=min(values) if values else 0.)


class NetAndLedgerExposureTracker:
	def __init__(self,tickers=()):
		self.net=OpenExposureTracker(tickers);self.virtual_ledger=OpenExposureTracker(tickers)
		self.daily_net={};self.daily_virtual_ledger={}
	def update(self,when,snapshot):
		net_peak=self.net.overall['max_open_market_value'];ledger_peak=self.virtual_ledger.overall['max_open_market_value']
		self.net.update(when,snapshot['net_by_ticker']);self.virtual_ledger.update(when,snapshot['virtual_ledger_by_ticker'])
		if self.net.overall['max_open_market_value']>net_peak:
			self.net.overall.update(virtual_ledger_market_value_at_peak=snapshot['gross_open_exposure'],signed_shares_by_ticker_at_peak=dict(snapshot['net_positions']))
		if self.virtual_ledger.overall['max_open_market_value']>ledger_peak:
			self.virtual_ledger.overall.update(net_market_value_at_peak=snapshot['net_open_exposure'])
		day=when.date().isoformat()
		self.daily_net[day]=max(self.daily_net.get(day,0.),snapshot['net_open_exposure'])
		self.daily_virtual_ledger[day]=max(self.daily_virtual_ledger.get(day,0.),snapshot['gross_open_exposure'])
	def daily_rows(self):
		return [dict(date=day,daily_max_capital_open_exposure=self.daily_net[day],daily_max_net_open_exposure=self.daily_net[day],daily_max_gross_open_exposure=self.daily_virtual_ledger[day]) for day in sorted(self.daily_net)]
	def summary(self):
		return dict(**exposure_summary(self.daily_net),capital_basis='net_positions',net_positions=exposure_summary(self.daily_net),virtual_ledger=exposure_summary(self.daily_virtual_ledger))
	def to_dict(self):
		# Unqualified capital metrics now mean net positions. Preserve gross explicitly.
		return copy.deepcopy(dict(**self.net.to_dict(),capital_basis='net_positions',net_positions=self.net.to_dict(),virtual_ledger=self.virtual_ledger.to_dict(),daily_max_open_exposure=self.daily_rows(),daily_max_open_exposure_summary=self.summary()))
	def to_text(self):
		net=self.net;ledger=self.virtual_ledger
		lines=['MAXIMUM OPEN-POSITION MARKET VALUE',
			'Capital basis: net positions within each stock, across all enabled strategy books.',
			'Net capital = sum across stocks of abs(long shares - short shares) x current stock price.',
			'Virtual ledger = gross market value before same-stock offsets. Disabled-strategy shadows excluded.',
			'Observed before exits and after the complete entry batch, at completed-minute/signal updates.',
			'Times include UTC offsets; peaks in the two views may occur at different times.',
			'',f"{'Metric':<32} {'Net positions (capital)':>25}   {'Virtual ledger (gross)':>25}",'-'*90,
			f"{'Overall simultaneous peak':<32} ${net.overall['max_open_market_value']:>24,.2f}  ${ledger.overall['max_open_market_value']:>24,.2f}",
			f"{'First observed at':<32} {net.overall['time'] or '--':>25}   {ledger.overall['time'] or '--':>25}",
			'',f"{'Stock':<10} {'Net peak':>17}   {'First observed at':<25}   {'Ledger peak':>17}   First observed at",'-'*110]
		for ticker in sorted(set(net.by_ticker)|set(ledger.by_ticker)):
			n=net.by_ticker[ticker];g=ledger.by_ticker[ticker]
			lines.append(f"{ticker:<10} ${n['max_open_market_value']:>16,.2f}   {n['time'] or '--':<25}  ${g['max_open_market_value']:>16,.2f}   {g['time'] or '--'}")
		for title,tracker,paired_key,paired_title in (('NET CAPITAL',net,'virtual_ledger_market_value_at_peak','Virtual ledger at this same moment'),('VIRTUAL LEDGER',ledger,'net_market_value_at_peak','Net capital at this same moment')):
			lines.extend(['',title+' SIMULTANEOUS PEAK HOLDINGS',f"First observed at: {tracker.overall['time'] or '--'}",f"{paired_title}: ${tracker.overall.get(paired_key,0.):,.2f}"])
			for ticker,value in sorted(tracker.overall['by_ticker_at_peak'].items()):lines.append(f'  {ticker:<10} ${value:,.2f}')
		lines.extend(['','DAILY SIMULTANEOUS PEAKS',f"{'Date':<14} {'Net positions (capital)':>25}   {'Virtual ledger (gross)':>25}"])
		for row in self.daily_rows():lines.append(f"{row['date']:<14} ${row['daily_max_net_open_exposure']:>24,.2f}  ${row['daily_max_gross_open_exposure']:>24,.2f}")
		summary=self.summary()
		lines.extend(['','DAILY PEAK STATISTICS (days with positive exposure in each view)',f"{'Statistic':<24} {'Net positions (capital)':>25}   {'Virtual ledger (gross)':>25}"])
		for label,key in (('Days with exposure','days_with_exposure'),('Mean','mean'),('Standard deviation','standard_deviation'),('Maximum','max'),('Minimum','min')):
			n,g=summary['net_positions'][key],summary['virtual_ledger'][key]
			lines.append(f'{label:<24} {n:>25,.2f}   {g:>25,.2f}')
		lines.extend(['','Individual-stock peaks are not added to obtain a simultaneous portfolio peak.',
			'Offsets apply within a stock only; long AAPL and short AMD do not cancel capital exposure.',
			'These are target inventory market values, not broker margin/buying-power requirements or cash balances.',
			'Pending orders, broker fill delays, share rounding and intraminute tick highs are not included.'])
		return '\n'.join(lines)+'\n'
