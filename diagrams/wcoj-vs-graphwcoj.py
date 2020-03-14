import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from diagrams.base import *

OUTPUT = True


def read_dataset(dataset_path, no_mat=False):
  data = pd.read_csv(dataset_path, sep=",", comment="#")
  fix_count(data)
  data["wcoj_time"] = (data["AlgoEnd-0"] - data["Scheduled-0"]) / 1000

  if no_mat:
    data["Algorithm"] = "NoMat"

  return data


def display_data(data, queries, annotate_speedup, output_name, legend=True):
  algs = ["WCOJ", "NoMat", "GraphWCOJ"]

  grouped = data.groupby(["Algorithm", "Query"])
  # return grouped
  median = grouped.median().round(2)
  error = grouped.std()

  if not "NoMat" in median.index:
    algs.remove("NoMat")

  wcoj_times = {}
  errors = {}

  for a in algs:
    wcoj_times[a] = list(map(lambda q: median["wcoj_time"][a][q], queries))
    errors[a] = list(map(lambda q: error["wcoj_time"][a][q], queries))
  speedup = list(map(lambda t: t[0] / t[1], zip(wcoj_times["WCOJ"], wcoj_times["GraphWCOJ"])))

  fig, ax = plt.subplots()

  x = np.arange(len(queries))

  width = 0.2
  algs_offset = {"WCOJ": -width, "GraphWCOJ": width, "NoMat": 0}
  algs_labels = {
    "WCOJ": "LFTJ",
    "NoMat": "LFTJ + CSR",
    "GraphWCOJ": "GraphWCOJ"
  }

  for a in algs:
    ax.bar(x + algs_offset[a], wcoj_times[a], yerr=errors[a], align="center", width=width, label=algs_labels[a])
    # ax.vlines(x + width + 0.1, wcoj_times["GraphWCOJ"], wcoj_times["WCOJ"])

  # if annotate_speedup:
  #   for i, l in enumerate(speedup):
  #     ax.annotate('%.1f' % speedup[i], xy = (x[i] + width + 0.2, wcoj_times["GraphWCOJ"][i]
  #                                   + (wcoj_times["WCOJ"][i] - wcoj_times["GraphWCOJ"][i]) / 2))

  if legend:
    plt.legend()
  plt.xticks(x, list(map(lambda s: s.capitalize(), queries)), rotation=45)
  plt.ylabel("Query runtime [s]")

  plt.grid(axis="y")

  plt.tight_layout()

  if OUTPUT:
    plt.savefig(FIGURE_PATH + output_name)

  plt.show()


data = read_dataset(DATASET_FOLDER + "final/sequential/snb-wcoj-graphwcoj.csv")
data = data.append(read_dataset(DATASET_FOLDER + "final/sequential/nomat-snb.csv", no_mat=True))
grouped = display_data(data, ["3-clique", "4-clique", "5-clique", "kite"], True, "lftj-graphWCOJ-snb.svg")

display_data(data, ["house", "diamond", "4-cycle"], True, "lftj-graphWCOJ-snb-long.svg", False)

# data = read_dataset(DATASET_FOLDER + "final/sequential/amazon-wcoj-graphwcoj.csv")
# data = data.append(read_dataset(DATASET_FOLDER + "final/sequential/nomat-amazon.csv", no_mat=True))
# display_data(data, ["3-clique", "4-clique", "5-clique", "kite"] + ["house", "diamond", "4-cycle"], True, "lftj-graphWCOJ-amazon.svg")
#
# data = read_dataset(DATASET_FOLDER + "final/sequential/amazon0601-wcoj-graphwcoj.csv")
# data = data.append(read_dataset(DATASET_FOLDER + "final/sequential/nomat-amazon0601.csv", no_mat=True))
# display_data(data, ["3-clique", "4-clique", "5-clique", "kite"], True, "lftj-graphWCOJ-amazon0601.svg")
# display_data(data, ["house", "diamond", "4-cycle"], True, "lftj-graphWCOJ-amazon0601-long.svg")

# data = read_dataset(DATASET_FOLDER + "final/sequential/wcoj-graphwcoj-twitter.csv")
# data = data.append(read_dataset(DATASET_FOLDER + "final/sequential/nomat-twitter.csv", no_mat=True))
# display_data(data, ["3-clique", "4-clique", "5-clique", "kite"], True, "lftj-graphWCOJ-twitter.svg")
# display_data(data, ["3-clique", "4-clique", "5-clique", "kite"], True, "lftj-graphWCOJ-twitter.svg")
