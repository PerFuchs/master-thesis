import pprint
import pandas as pd
from collections import defaultdict, OrderedDict
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from diagrams.base import *

OUTPUT = True

def read_dataset(dataset_path):
  data = pd.read_csv(dataset_path, sep=",", comment="#")
  fix_count(data)
  split_partitioning(data)
  add_wcoj_time(data)
  add_worker_times(data)
  add_skew(data)
  return data


def output_scaling_graph(data, output_path):
  grouped = data.groupby(["partitioning_base", "Query", "Parallelism"])
  queries = ["3-clique"]

  partitionings = ["Shares", "SharesRange", "SharesRangeMulti", "1-variable", "2-variable"]

  parallelism_levels = [1, 4, 8, 16, 32, 48]
  parallelism_levels.sort()
  base_scaling_level = parallelism_levels[0]

  median = grouped.median()

  scaling = defaultdict(lambda: list())

  for pa in partitionings:
    if pa != "AllTuples":
      for q in queries:
        for (i, p) in enumerate(parallelism_levels):
          if p == base_scaling_level:
            scaling[(pa, q)].append(p)
          elif base_scaling_level < p:
            scaling[(pa, q)].append(median["wcoj_time"]["AllTuples"][q][base_scaling_level]
                                    / median["wcoj_time"][pa][q][p])
  colors = {"Shares": "C0",
            "linear": "C7",
            "SharesRange": "C1",
            "1-variable": "C2",
            "2-variable": "C3",
            "SharesRangeMulti": "C4"}

  markers = {"3-clique": "o", "5-clique": "^", "4-clique": "d"}

  plots = OrderedDict()
  for p in partitionings:
    if p != "AllTuples":
      for q in queries:
        plots[(p, q)] = plt.scatter(
          parallelism_levels,
          scaling[(p, q)],
          color=colors[p],
          marker=markers[q]
        )

  linear_plot = plt.plot(parallelism_levels, parallelism_levels, color=colors["linear"], linestyle="--")[0]

  legend = OrderedDict()
  legend["linear"] = linear_plot
  for k, p in plots.items():
    legend[k[0]] = p

  plt.legend(list(legend.values()), list(legend.keys()))
  plt.xticks(list(filter(lambda p: p != 2, parallelism_levels)))

  plt.xlabel("\\# Workers")
  plt.ylabel("Speedup")

  plt.grid(axis="y")

  plt.tight_layout()
  if OUTPUT:
    plt.savefig(FIGURE_PATH + output_path)
  plt.show()
  plt.clf()
  return data


def output_skew_graph(data, output_path):
  grouped = data.groupby(["partitioning_base", "Query", "Parallelism"])
  queries = ["3-clique"]

  partitionings = ["Shares", "SharesRange", "1-variable", "2-variable", "SharesRangeMulti"]

  parallelism_levels = [4, 8, 16, 32, 48]

  median = grouped.median()

  colors = {"Shares": "C0",
            "linear": "C7",
            "SharesRange": "C1",
            "1-variable": "C2",
            "2-variable": "C3",
            "SharesRangeMulti": "C4"}
  markers = {"3-clique": "o", "5-clique": "^", "4-clique": "d"}

  plots = OrderedDict()
  for p in partitionings:
    if p != "AllTuples":
      for q in queries:
        skew = []
        for para in parallelism_levels:
            skew.append(median["skew"][p][q][para])
        plots[(p, q)] = plt.scatter(
          parallelism_levels,
          skew,
          color=colors[p],
          marker=markers[q]
        )

  legend = OrderedDict()
  for k, p in plots.items():
    legend[k[0] + ", " + k[1]] = p

  # plt.legend(list(legend.values()), list(legend.keys()))

  plt.xticks(list(filter(lambda p: p != 2, parallelism_levels)))

  plt.xlabel("Total number of threads")
  plt.ylabel("Skew")

  plt.grid(axis="y")

  plt.tight_layout()
  if OUTPUT:
    plt.savefig(FIGURE_PATH + output_path)
  plt.show()
  plt.clf()

# data = read_dataset(DATASET_FOLDER + "final/graphWCOJ-scaling/twitter-scaling.csv")
# data_alltuples = data[data["partitioning_base"] == "AllTuples"]
# data_shares = data[data["partitioning_base"] == "Shares"]
# data = data_shares.append(data_alltuples)

data = read_dataset(DATASET_FOLDER + "final/graphWCOJ-scaling/other-schemes.csv")
data = data.append(read_dataset(DATASET_FOLDER + "final/graphWCOJ-scaling/shares-range-multi.csv"))


output_scaling_graph(data, "graphWCOJ-scaling-other-schemes.svg")
output_skew_graph(data, "graphWCOJ-scaling-other-schemes-skew.svg")