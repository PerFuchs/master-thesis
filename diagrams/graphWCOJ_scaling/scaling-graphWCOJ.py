import pprint
import pandas as pd
from collections import defaultdict, OrderedDict

from diagrams.base import *


def output_table_and_graph(dataset_path, parallelism_levels_5_clique_workstealing, parallelism_levels_5_clique_shares, output_path):
  data = pd.read_csv(dataset_path, sep=",", comment="#")

  fix_count(data)
  split_partitioning(data)

  data["total_time"] = data["End"] - data["Start"]

  grouped = data.groupby(["partitioning_base", "Query", "Parallelism"])
  queries = list(set(data["Query"]))

  partitionings = list(set(data["partitioning_base"]))
  if "SharesRange" in partitionings:
    partitionings.remove("SharesRange")

  parallelism_levels = list(set(data["Parallelism"]))
  parallelism_levels.sort()

  median = grouped.median()

  scaling = defaultdict(lambda: list())

  for pa in partitionings:
    if pa != "AllTuples":
      for q in queries:
        para_levels = parallelism_levels
        if q == "5-clique" and pa == SHARES:
          para_levels = parallelism_levels_5_clique_shares
        if q == "5-clique" and pa == WORKSTEALING:
          para_levels = parallelism_levels_5_clique_workstealing
        base_scaling_level = para_levels[0]
        for (i, p) in enumerate(para_levels):
          if p == base_scaling_level:
            scaling[(pa, q)].append(p)
          elif base_scaling_level < p:
            scaling[(pa, q)].append(median["total_time"]["AllTuples" if base_scaling_level == 1 else pa][q][base_scaling_level]
                                    / median["total_time"][pa][q][p] * base_scaling_level)
  colors = {"Shares": "C0", "FirstVariablePartitioningWithWorkstealing": "C1"}
  markers = {"3-clique": "o", "5-clique": "x"}
  plots = {}
  for p in partitionings:
    if p != "AllTuples":
      for q in queries:
        para_levels = parallelism_levels
        if p == SHARES and q == "5-clique":
          para_levels = parallelism_levels_5_clique_shares
        elif p == WORKSTEALING and q == "5-clique":
          para_levels = parallelism_levels_5_clique_workstealing
        plots[(p, q)] = plt.scatter(
          para_levels,
          scaling[(p, q)],
          color=colors[p],
          marker=markers[q]
        )

  linear_plot = plt.plot(parallelism_levels, parallelism_levels, color="C7")[0]

  legend = OrderedDict()
  legend["linear"] = linear_plot
  legend["work-stealing, 3-clique"] = plots[(WORKSTEALING, "3-clique")]
  legend["Shares, 3-clique"] = plots[(SHARES, "3-clique")]
  legend["work-stealing, 5-clique"] = plots[(WORKSTEALING, "5-clique")]
  legend["Shares, 5-clique"] = plots[(SHARES, "5-clique")]

  plt.legend(list(legend.values()), list(legend.keys()))
  plt.xticks(list(filter(lambda p: p != 2, parallelism_levels)))

  plt.xlabel("\\# Workers")
  plt.ylabel("Speedup")

  plt.grid(axis="y")

  plt.tight_layout()
  plt.savefig(FIGURE_PATH + output_path)
  plt.show()
  plt.clf()

  rows = []
  for (k, v) in scaling.items():
    pa = k[0]
    q = k[1]
    para_levels = parallelism_levels
    if pa == SHARES and q == "5-clique":
      para_levels = parallelism_levels_5_clique_shares
    elif pa == WORKSTEALING and q == "5-clique":
      para_levels = parallelism_levels_5_clique_workstealing
    for i, s in enumerate(v):
      p = para_levels[i]
      time = median["total_time"][pa if p != 1 else "AllTuples"][q][p] / 60000
      rows.append([partitioning_names[pa], q, p, time, s])

  table = pd.DataFrame(rows, columns=("Partitioning", "Query", "Parallelism", "Time", "Speedup"))
  table = table.sort_values(["Partitioning", "Query", "Parallelism"])
  table = table.round(1)

  tabulize_data(table, GENERATED_PATH + output_path.replace(".svg", ".tex"))


def tabulize_data(data, output_path):
  data.to_latex(buf=open(output_path, "w"),
                # columns=["Par", "Count", "Time", "WCOJTime_wcoj", "setup", "ratio"],
                # header = ["Query", "\\# Result", "\\texttt{BroadcastHashJoin}", "\\texttt{seq}", "setup", "Speedup"],
                column_format="llr|rr",
                # formatters = {
                  # "ratio": lambda r: str(round(r, 1)),
                #   "Count": lambda c: "{:,}".format(c),
                # },
                escape=True,
                index=False
                )
FIX_DATASET_PATH = DATASET_FOLDER + "final/graphWCOJ-scaling/orkut-5-clique-1.csv"

# fix_shares(FIX_DATASET_PATH, DATASET_FOLDER +
#            FIX_DATASET_PATH + ".shares-fixed")
# fix_missing_columns(FIX_DATASET_PATH + ".shares-fixed",
#                     FIX_DATASET_PATH + ".fixed", 96)

DATASET_ORKUT = DATASET_FOLDER + "final/graphWCOJ-scaling/orkut-scaling.csv"
DATASET_LIVEJ = DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-scaling.csv"
DATASET_TWITTER = DATASET_FOLDER + "final/graphWCOJ-scaling/twitter-scaling.csv"

output_table_and_graph(DATASET_ORKUT, [1, 16, 32, 64, 96], [1, 16, 32, 64, 96], "graphWCOJ-scaling-orkut.svg")
output_table_and_graph(DATASET_LIVEJ, [1, 16, 32, 48, 64, 96], [1, 16, 32, 48, 64, 96], "graphWCOJ-scaling-livej.svg")
output_table_and_graph(DATASET_TWITTER, [1, 2, 4, 8, 16, 32, 48, 64, 96], [1, 2, 4, 8, 16, 32, 48, 64, 96], "graphWCOJ-scaling-twitter.svg")
