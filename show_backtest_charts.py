from pathlib import Path

import matplotlib.image as mpimg
import matplotlib.pyplot as plt


def show_charts_sequentially(
	directory: str,
) -> None:
	chart_directory = Path(
		directory
	)

	image_paths = sorted(
		chart_directory.glob("*.png")
	)

	if not image_paths:
		raise ValueError(
			f"No PNG charts found in {chart_directory}"
		)

	for image_path in image_paths:
		image = mpimg.imread(
			image_path
		)

		fig, ax = plt.subplots(
			figsize=(14, 8)
		)

		ax.imshow(
			image
		)

		ax.set_title(
			image_path.stem.replace(
				"_",
				" ",
			)
		)

		ax.axis(
			"off"
		)

		fig.tight_layout()

		plt.show(
			block=True
		)

		plt.close(
			fig
		)


if __name__ == "__main__":
	show_charts_sequentially(
		"backtest_charts"
	)