import pprint
import pandas as pd
from collections import defaultdict, OrderedDict
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from diagrams.base import *

OUTPUT = True

def output_table_and_graph(dataset_path, output_path):
  data = pd.read_csv(dataset_path, sep=",", comment="#")

  fix_count(data)
  split_partitioning(data)
  add_wcoj_time(data)

  data["total_time"] = data["End"] - data["Start"]

  grouped = data.groupby(["partitioning_base", "Query", "Parallelism"])
  queries = list(set(data["Query"]))

  partitionings = list(set(data["partitioning_base"]))
  if "SharesRange" in partitionings:
    partitionings.remove("SharesRange")
  if "1-variable" in partitionings:
    partitionings.remove("1-variable")


  parallelism_levels = list(set(data["Parallelism"]))
  parallelism_levels.sort()

  median = grouped.median()

  scaling = defaultdict(lambda: list())

  for pa in partitionings:
    if pa != "AllTuples":
      for q in queries:
        para_levels = parallelism_levels
        base_scaling_level = para_levels[0]
        for (i, p) in enumerate(para_levels):
          if p == base_scaling_level:
            scaling[(pa, q)].append(p)
          elif base_scaling_level < p:
            scaling[(pa, q)].append(median["wcoj_time"]["AllTuples" if base_scaling_level == 1 else pa][q][base_scaling_level]
                                    / median["wcoj_time"][pa][q][p] * base_scaling_level)
  colors = {"Shares": "C0", "FirstVariablePartitioningWithWorkstealing": "C1"}
  markers = {"3-clique": "o", "5-clique": "^", "4-clique": "d"}
  plots = {}

  plots[(WORKSTEALING, "3-clique")] = plt.scatter(
        para_levels,
        scaling[(WORKSTEALING, "3-clique")],
        color=colors[WORKSTEALING],
        marker=markers["3-clique"]
      )

  linear_plot = plt.plot(parallelism_levels, parallelism_levels, color="C7")[0]

  legend = OrderedDict()
  legend["linear"] = linear_plot
  legend["work-stealing"] = Patch(facecolor=colors[WORKSTEALING])

  legend["3-clique"] = Line2D([0], [0], marker=markers[q], color='w', markerfacecolor='black')

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

DATASET_TWITTER = DATASET_FOLDER + "final/graphWCOJ-scaling/twitter-scaling.csv"

data = output_table_and_graph(DATASET_TWITTER, "twitter-3-clique-scaling.svg")
add_spark_overhead(data)

m = data.groupby(["partitioning_base", "Query", "Parallelism"]).median()
print("Spark overhead", m["spark_overhead"][WORKSTEALING]["3-clique"][48] / 1000)
print("Time 48", m["total_time"][WORKSTEALING]["3-clique"][48] / 1000)
