import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from diagrams.base import *

OUTPUT = True


def read_dataset(dataset_path):
  data = pd.read_csv(dataset_path, sep=",", comment="#")
  # fix_count(data)


  data["wcoj_time"] = (data["AlgoEnd-0"] - data["Scheduled-0"]) / 1000
  data["spark_time"] = (data["End"] - data["Start"]) / 1000

  data = data.groupby(["Algorithm", "Query"])
  data = data.median().round(2)

  return data


def display_data(data, queries, annotate_speedup, output_name):
  algs = ["WCOJ", "BroadcastHashJoin"]

  times = {}
  for a in algs:
    times[a] = list(map(lambda q: data["wcoj_time"][a][q] if a == "WCOJ" else data["spark_time"][a][q], queries))
  speedup = list(map(lambda t: t[0] / t[1], zip(times["BroadcastHashJoin"], times["WCOJ"])))

  fig, ax = plt.subplots()

  x = np.arange(len(queries))

  width = 0.2
  algs_offset = {"WCOJ": width/2, "BroadcastHashJoin": -width/2}

  for a in algs:
    ax.bar(x + algs_offset[a], times[a], align="center", width=width, label=a)
    ax.vlines(x + width + 0.1, times["WCOJ"], times["BroadcastHashJoin"])

  if annotate_speedup:
    for i, l in enumerate(speedup):
      ax.annotate('%.1f' % speedup[i], xy = (x[i] + width + 0.2, times["WCOJ"][i]
                                             + (times["BroadcastHashJoin"][i] - times["WCOJ"][i]) / 2))

  plt.legend()
  plt.xticks(x, queries, rotation=45)
  plt.ylabel("runtime time [s]")

  plt.grid(axis="y")

  plt.tight_layout()

  if OUTPUT:
    plt.savefig(FIGURE_PATH + output_name)

  plt.show()


data = read_dataset(DATASET_FOLDER + "final/sequential/amazon-wcoj-graphwcoj.csv")
data = data.append(read_dataset(DATASET_FOLDER + "final/sequential/spark-amazon.csv"))
data = data.append(read_dataset(DATASET_FOLDER + "final/sequential/paths-amazon.csv"), sort=False)
display_data(data, ["3-clique", "4-clique", "5-clique", "kite", "3-0.00-path", "4-0.00-path"], True, "spark-wcoj-amazon.svg")
display_data(data, ["house", "diamond", "4-cycle"], True, "spark-wcoj-amazon-long.svg")


data = read_dataset(DATASET_FOLDER + "final/sequential/amazon0601-wcoj-graphwcoj.csv")
data = data.append(read_dataset(DATASET_FOLDER + "final/sequential/spark-amazon0601.csv"))
data = data.append(read_dataset(DATASET_FOLDER + "final/sequential/paths-amazon0601.csv"), sort=False)
display_data(data, ["3-clique", "4-clique", "5-clique", "kite", "3-0.00-path", "4-0.00-path"], True, "spark-wcoj-amazon0601.svg")
display_data(data, ["house", "diamond", "4-cycle"], True, "spark-wcoj-amazon0601-long.svg")

data = read_dataset(DATASET_FOLDER + "final/sequential/snb-wcoj-graphwcoj.csv")
data = data.append(read_dataset(DATASET_FOLDER + "final/sequential/spark-snb1.csv"))
data = data.append(read_dataset(DATASET_FOLDER + "final/sequential/paths-snb.csv"), sort=False)
display_data(data, ["3-clique", "4-clique", "5-clique", "kite", "3-0.00-path"], True, "spark-wcoj-snb.svg")
display_data(data, ["house", "diamond", "4-cycle", "4-0.00-path"], True, "spark-wcoj-snb-long.svg")

# TODO error bars