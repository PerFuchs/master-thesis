import pandas as pd
import matplotlib.pyplot as plt

from diagrams.base import *

DATASET = DATASET_FOLDER + "twitter-scaling.csv"

data = pd.read_csv(DATASET, sep=",", comment="#")

fix_count(data)

data["total_time"] = data["End"] - data["Start"]



grouped = data.groupby(["Parallelism"])
parallelism_levels = list(set(data["Parallelism"]))
parallelism_levels.sort()


median = grouped.median()

scaling = []

for (i, p) in enumerate(parallelism_levels):
  if (p == 1):
    scaling.append(1)
  else:
    scaling.append(median["total_time"][1] / median["total_time"][p])

plt.scatter(parallelism_levels, scaling)
plt.plot(parallelism_levels, parallelism_levels)


plt.xticks(parallelism_levels)

# grouped = displayData.groupby("Query")


errors = grouped.std()

# a = median["total_time"].plot.line(yerr=errors, capsize=5)
# autolabel(a.patches, "right")

plt.xlabel("\\# Workers")
plt.ylabel("Speedup")
# plt.xticks(rotation=45)

axes = plt.gca()
plt.grid(axis="x")

plt.tight_layout()

# plt.savefig(FIGURE_PATH + "seq-bar-ama0302.svg")
plt.show()