"""Print/save a run's small exposure report without loading its large history."""
import json
from pathlib import Path
import sys
import zipfile

def read_json_report(stream):
	# The simulation deliberately returns this summary as its first JSON field.
	# Read only the prefix, then decode the report string, never the history arrays.
	buffer=stream.read(65536)
	start=len(buffer)-len(buffer.lstrip())
	if start>=len(buffer) or buffer[start]!='{': return None
	decoder=json.JSONDecoder()
	position=start+1
	while position<len(buffer) and buffer[position].isspace(): position+=1
	try: key,end=decoder.raw_decode(buffer,position)
	except json.JSONDecodeError: return None
	if key!='max_open_positions_report': return None
	position=end
	while position<len(buffer) and buffer[position].isspace(): position+=1
	if position>=len(buffer) or buffer[position]!=':': return None
	position+=1
	while position<len(buffer) and buffer[position].isspace(): position+=1
	while True:
		try:
			value,end=decoder.raw_decode(buffer,position)
			return value if isinstance(value,str) else None
		except json.JSONDecodeError:
			chunk=stream.read(65536)
			if not chunk: return None
			buffer+=chunk

def extract_report(path):
	if zipfile.is_zipfile(path):
		with zipfile.ZipFile(path) as archive:
			if 'max_open_positions_report.txt' not in archive.namelist(): return None
			return archive.read('max_open_positions_report.txt').decode('utf-8')
	with Path(path).open(encoding='utf-8') as stream: return read_json_report(stream)

def main():
	report=extract_report(sys.argv[1])
	if report is None: return
	destination=Path(sys.argv[2]);destination.parent.mkdir(parents=True,exist_ok=True)
	destination.write_text(report,encoding='utf-8')
	print('\n'+report,end='' if report.endswith('\n') else '\n')
	print('\nMaximum open-position report saved to:\n\n    '+str(destination)+'\n')

if __name__=='__main__': main()
