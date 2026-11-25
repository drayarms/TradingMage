"""Authenticated Alpaca calendar recovery without inventing exchange sessions."""
import logging,re,time
from urllib.parse import urlsplit

logger=logging.getLogger('tv-webhook')
CACHE_SECONDS=6*60*60


def status_code(exc):
	status=getattr(exc,'status_code',None)
	if status is None:status=getattr(getattr(exc,'response',None),'status_code',None)
	try:return int(status) if status is not None else None
	except (ValueError,TypeError):return None


def safe_error(exc,api):
	message=str(exc)
	for name in ('_key_id','_secret_key','_oauth'):
		secret=getattr(api,name,None)
		if isinstance(secret,str) and secret:message=message.replace(secret,'[redacted]')
	message=re.sub(r'Bearer\s+[^\s,;]+','Bearer [redacted]',message,flags=re.I)
	def url(match):
		try:
			parts=urlsplit(match.group());return parts.scheme+'://'+str(parts.hostname or '[host]')+parts.path
		except ValueError:return '[URL]'
	message=re.sub(r'https?://[^\s\'"<>]+',url,message)
	message=' '.join(message.split())[:240]
	status=status_code(exc)
	return type(exc).__name__+(f' HTTP {status}' if status else '')+(': '+message if message else '')


def transient(exc):
	status=status_code(exc)
	return status in {408,425,429} or status is not None and 500<=status<=599 or isinstance(exc,(TimeoutError,ConnectionError)) or type(exc).__name__ in {'Timeout','ReadTimeout','ConnectTimeout','ConnectionError','SSLError','ChunkedEncodingError'}


def fetch_calendar(api,start,end,parse,now=None):
	"""Cache only successfully parsed complete responses, keyed by API object/range."""
	clock=time.monotonic() if now is None else now
	cache=getattr(api,'_tradingmage_calendar_cache',None)
	if isinstance(cache,dict) and 0<=clock-cache.get('created_at',float('-inf'))<CACHE_SECONDS and cache.get('start','9999')<=start and cache.get('end','0000')>=end:
		return parse(cache['rows'])
	failure=None
	for attempt in range(3):
		try:
			rows=list(api.get_calendar(start=start,end=end));calendar=parse(rows)
			break
		except Exception as exc:
			failure=exc
			if not transient(exc) or attempt==2:rows=None;break
			logger.warning('Alpaca calendar attempt %s failed for %s..%s: %s; retrying',attempt+1,start,end,safe_error(exc,api))
			time.sleep(.25*(attempt+1))
	if rows is None:
		# Same authenticated trading endpoint; bypass legacy SDK entity conversion.
		get=getattr(api,'get',None)
		if callable(get) and status_code(failure) is None:
			try:
				rows=list(get('/calendar',{'start':start,'end':end}));calendar=parse(rows)
				logger.warning('Alpaca calendar recovered via raw authenticated response after %s',safe_error(failure,api))
			except Exception as exc:
				raise ValueError(safe_error(failure,api)+'; raw calendar: '+safe_error(exc,api)) from None
		else:raise ValueError(safe_error(failure,api)) from None
	try:api._tradingmage_calendar_cache=dict(start=start,end=end,rows=rows,created_at=clock)
	except (AttributeError,TypeError):pass
	return calendar
