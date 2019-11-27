import pprint
import pandas as pd
from collections import defaultdict, OrderedDict
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from diagrams.base import *

OUTPUT = True

def add_allTuplesPartitioning(data, dataset_path):
  if "orkut" in dataset_path:
    dataset = "orkut"
  else:
    dataset = "liveJ"

  all_tuple_paths = {
    "orkut": DATASET_FOLDER + "final/graphWCOJ-scaling/orkut-scaling.csv",
   "liveJ": DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-scaling.csv"
  }

  all_tuples = pd.read_csv(all_tuple_paths[dataset], sep=",", comment="#")
  split_partitioning(all_tuples)
  all_tuples = all_tuples[all_tuples["partitioning_base"] == "AllTuples"]

  data = data.append(all_tuples, sort=False)
  return data


def output_table_and_graph(dataset_path, parallelism_levels_5_clique_workstealing, output_path):
  data = pd.read_csv(dataset_path, sep=",", comment="#")
  queries = list(set(data["Query"]))

  data = add_allTuplesPartitioning(data, dataset_path)

  fix_count(data)
  split_partitioning(data)
  add_wcoj_time(data)

  data["total_time"] = data["End"] - data["Start"]

  grouped = data.groupby(["partitioning_base", "Query", "Parallelism"])
  # return grouped


  partitionings = list(set(data["partitioning_base"]))

  parallelism_levels = list(set(data["Parallelism"]))
  parallelism_levels.sort()

  median = grouped.median()

  scaling = defaultdict(lambda: list())

  for pa in partitionings:
    if pa != "AllTuples":
      for q in queries:
        if "batched" in pa and q == "5-clique":
          continue
        para_levels = parallelism_levels
        if q == "5-clique" and pa == WORKSTEALING:
          para_levels = parallelism_levels_5_clique_workstealing
        base_scaling_level = para_levels[0]
        for (i, p) in enumerate(para_levels):
          if p == base_scaling_level:
            scaling[(pa, q)].append(p)
          elif base_scaling_level < p:
            print(pa, q, p)
            scaling[(pa, q)].append(median["total_time"]["AllTuples"][q][1] / (median["total_time"][pa][q][p]))
  print(scaling)

  colors = {"Shares": "C0", "FirstVariablePartitioningWithWorkstealing": "C1", "FirstVariablePartitioningWithWorkstealing-batched": "C2" }
  markers = {"3-clique": "o", "5-clique": "^"}
  plots = OrderedDict()
  for p in partitionings:
    if p != "AllTuples":
      for q in queries:
        if q =="5-clique" and "batched" in p:
          continue
        para_levels = parallelism_levels
        if p == WORKSTEALING and q == "5-clique":
          para_levels = parallelism_levels_5_clique_workstealing
        plots[(p, q)] = plt.scatter(
          para_levels,
          scaling[(p, q)],
          color=colors[p],
          marker=markers[q]
        )

  linear_plot = plt.plot(parallelism_levels, parallelism_levels, color="C7")[0]

  legend_labels = {
    (WORKSTEALING, "3-clique"): "3-clique",
    (WORKSTEALING + "-batched", "3-clique"): "3-clique batched",
    (WORKSTEALING, "5-clique"): "5-clique"
  }

  legend = OrderedDict()
  legend["linear"] = linear_plot

  for k, v in plots.items():
    legend[legend_labels[k]] = v


  # sorted(queries, reverse=True)
  # for q in queries:
  #   legend[q] = Line2D([0], [0], marker=markers[q], color='w', markerfacecolor='black')


  plt.legend(list(legend.values()), list(legend.keys()))
  plt.xticks(list(filter(lambda p: p != 2 and p != 32 and p != 64, parallelism_levels)))

  plt.xlabel("\\# Workers")
  plt.ylabel("Speedup")

  plt.grid(axis="y")

  plt.tight_layout()
  if OUTPUT:
    plt.savefig(FIGURE_PATH + output_path)
  plt.show()
  plt.clf()

  rows = []
  for (k, v) in scaling.items():
    pa = k[0]
    q = k[1]
    para_levels = parallelism_levels
    if pa == WORKSTEALING and q == "5-clique":
      para_levels = parallelism_levels_5_clique_workstealing
    for i, s in enumerate(v):
      p = para_levels[i]
      time = median["total_time"][pa if p != 1 else "AllTuples"][q][p] / 1000
      rows.append([partitioning_names[pa], q, p, time, s])

  table = pd.DataFrame(rows, columns=("Partitioning", "Query", "Parallelism", "Time", "Speedup"))
  table = table.sort_values(["Partitioning", "Query", "Parallelism"])
  table = table.round(1)

  tabulize_data(table, GENERATED_PATH + output_path.replace(".svg", ".tex"))

  return data


def tabulize_data(data, output_path):
  data.to_latex(buf=open(output_path, "w"),
                # columns=["Par", "Count", "Time", "WCOJTime_wcoj", "setup", "ratio"],
                # header = ["Query", "\\# Result", "\\texttt{BroadcastHashJoin}", "\\texttt{seq}", "setup", "Speedup"],
                column_format="llr|rr",
                longtable=True,
                # formatters = {
                # "ratio": lambda r: str(round(r, 1)),
                #   "Count": lambda c: "{:,}".format(c),
                # },
                escape=True,
                index=False
                )


DATASET_ORKUT = DATASET_FOLDER + "final/distributed/orkut.csv"
DATASET_LIVEJ = DATASET_FOLDER + "final/distributed/liveJ.csv"

output_table_and_graph(DATASET_ORKUT, [1, 16, 32, 48, 64, 96, 128, 192, 384], "distributed-scaling-orkut.svg")

data = output_table_and_graph(DATASET_LIVEJ, [1, 16, 32, 48, 64, 96, 128, 192, 384], "distributed-scaling-livej.svg")
