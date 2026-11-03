"""Historical Alpaca calendar gates for regular and extended-hours execution."""
from datetime import datetime,time,timedelta
from zoneinfo import ZoneInfo
import pandas as pd

EASTERN=ZoneInfo('America/New_York')

def field(row,name,default=None):
	return row.get(name,default) if isinstance(row,dict) else getattr(row,name,default)

def at(day,value):
	if isinstance(value,time): return pd.Timestamp(datetime.combine(day,value.replace(tzinfo=None)),tz=EASTERN)
	s=str(value).strip()
	if len(s)==4 and s.isdigit(): s=s[:2]+':'+s[2:]
	if ':' in s and len(s)<=8: return pd.Timestamp(str(day)+'T'+s,tz=EASTERN)
	t=pd.Timestamp(value)
	return t.tz_localize(EASTERN) if t.tzinfo is None else t.tz_convert(EASTERN)

class BacktestSessions:
	def __init__(self,rows):
		self.days={}
		for row in rows:
			day=pd.Timestamp(field(row,'date')).date()
			if day.weekday()>=5: continue
			core_open=at(day,field(row,'open'));core_close=at(day,field(row,'close'))
			# Alpaca's legacy calendar includes the actual extended session bounds.
			start=at(day,field(row,'session_open') or '04:00')
			end=at(day,field(row,'session_close')) if field(row,'session_close') else min(at(day,'20:00'),core_close+pd.Timedelta(hours=4))
			start=max(start,at(day,'04:00'));end=min(end,at(day,'20:00'))
			if not start<=core_open<core_close<=end: raise ValueError('Invalid Alpaca market calendar session')
			self.days[day]=(start,core_open,core_close,end)

	@classmethod
	def load(cls,api,start,end):
		try: rows=api.get_calendar(start=(pd.Timestamp(start).date()-timedelta(days=1)).isoformat(),end=(pd.Timestamp(end).date()+timedelta(days=1)).isoformat())
		except Exception as exc: raise ValueError('Cannot enforce trading hours: Alpaca calendar unavailable') from exc
		return cls(rows)

	def bounds(self,when):
		t=pd.Timestamp(when)
		if t.tzinfo is None: raise ValueError('Execution timestamps must be timezone-aware')
		t=t.tz_convert(EASTERN)
		return t,self.days.get(t.date())

	def allows(self,when):
		t,b=self.bounds(when)
		return b is not None and b[0]<=t<b[3]

	def extended(self,when):
		t,b=self.bounds(when)
		return b is not None and self.allows(t) and not b[1]<=t<b[2]

	def same_session(self,source,execution):
		s,b=self.bounds(source);t,c=self.bounds(execution)
		return b is not None and c is not None and s.date()==t.date() and b[0]<=s and s+pd.Timedelta(minutes=1)<=t<c[3]
