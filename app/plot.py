"""
from io import BytesIO

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


class Plot:
	def plot_pnl_history(self, history, title="PnL History"):
		fig, ax = plt.subplots(figsize=(11, 5))

		x = [item["snapshot_time"] for item in history]
		y = [item["total_pnl"] for item in history]

		ax.plot(x, y)
		ax.set_title(title)
		ax.set_xlabel("Snapshot Time")
		ax.set_ylabel("PnL")
		ax.grid(True)
		ax.tick_params(axis="x", rotation=45)

		fig.tight_layout()

		buffer = BytesIO()
		fig.savefig(buffer, format="png")
		buffer.seek(0)
		plt.close(fig)

		return buffer
"""


from io import BytesIO
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


class Plot:
	def plot_pnl_history(self, history, title="PnL History"):
		if not history:
			raise ValueError("No history to plot")

		parsed = []
		for item in history:
			snapshot_time = item.get("snapshot_time")
			total_pnl = item.get("total_pnl")

			if snapshot_time in (None, "") or total_pnl in (None, ""):
				continue

			try:
				x_val = datetime.fromisoformat(str(snapshot_time).replace("Z", "+00:00"))
				y_val = float(total_pnl)
				parsed.append((x_val, y_val))
			except Exception:
				continue

		if not parsed:
			raise ValueError("No valid history points to plot")

		parsed.sort(key=lambda p: p[0])

		x = [p[0] for p in parsed]
		y = [p[1] for p in parsed]

		fig, ax = plt.subplots(figsize=(11, 5))

		ax.plot(x, y)
		ax.set_title(title)
		ax.set_xlabel("Snapshot Time")
		ax.set_ylabel("PnL")
		ax.grid(True)

		ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
		fig.autofmt_xdate(rotation=45)

		fig.tight_layout()

		buffer = BytesIO()
		fig.savefig(buffer, format="png")
		buffer.seek(0)
		plt.close(fig)

		return buffer