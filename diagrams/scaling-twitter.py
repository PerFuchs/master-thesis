import pandas as pd
from collections import defaultdict

from diagrams.base import *

DATASET = DATASET_FOLDER + "final/orkut-scaling-clique3.csv"

data = pd.read_csv(DATASET, sep=",", comment="#")

fix_count(data)
split_partitioning(data)

data["total_time"] = data["End"] - data["Start"]

grouped = data.groupby(["partitioning_base", "Query", "Parallelism"])
queries = list(set(data["Query"]))
partitionings = list(set(data["partitioning_base"]))
parallelism_levels = list(set(data["Parallelism"]))
parallelism_levels.sort()

median = grouped.median()

scaling = defaultdict(lambda: list())

for pa in partitionings:
  if pa != "AllTuples":
    for q in queries:
      for (i, p) in enumerate(parallelism_levels):
        if p == 1:
          scaling[(pa, q)].append(1)
        else:
          scaling[(pa, q)].append(median["total_time"]["AllTuples"][q][1] / median["total_time"][pa][q][p])


plots = {}
for p in partitionings:
  if p != "AllTuples":
    for q in queries:
      plots[(p, q)] = plt.scatter(parallelism_levels, scaling[(p, q)])
plt.plot(parallelism_levels, parallelism_levels)

plt.legend(list(plots.values()), list(plots.keys()))
plt.xticks(parallelism_levels)


plt.xlabel("\\# Workers")
plt.ylabel("Speedup")
# plt.xticks(rotation=45)

axes = plt.gca()
plt.grid(axis="x")

plt.tight_layout()

# plt.savefig(FIGURE_PATH + "seq-bar-ama0302.svg")
plt.show()


def tabulize_data(data, output_path):
  data.to_latex(buf=open(output_path, "w"),
                # columns=["Par", "Count", "Time", "WCOJTime_wcoj", "setup", "ratio"],
                # header = ["Query", "\\# Result", "\\texttt{BroadcastHashJoin}", "\\texttt{seq}", "setup", "Speedup"],
                column_format="lr||r|rr||r",
                # formatters = {
                #   "ratio": lambda r: str(round(r, 1)),
                #   "Count": lambda c: "{:,}".format(c),
                # },
                escape=True,
                index=False
                )


rows = []
for (k, v) in scaling.items():
  pa = k[0]
  q = k[1]
  for i,s in enumerate(v):
    rows.append([pa, q, parallelism_levels[i], 0.0, s])


table = pd.DataFrame(rows, columns=("Partitioning", "Query", "Parallelism", "Time", "Scaling"))
table = table.sort_values(["Partitioning", "Query", "Parallelism"])

tabulize_data(table, GENERATED_PATH + "twitter-scaling.tex")
