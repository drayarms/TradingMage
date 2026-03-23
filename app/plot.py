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