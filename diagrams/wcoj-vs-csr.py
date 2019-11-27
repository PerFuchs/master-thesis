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

  data = data.groupby(["Algorithm", "Query"])
  data = data.median().round(2)

  return data


def display_data(data, queries, output_name):
  algs = ["WCOJ", "NoMat"]

  wcoj_times = {}
  for a in algs:
    wcoj_times[a] = list(map(lambda q: data["wcoj_time"][a][q], queries))

  fig, ax = plt.subplots()

  x = np.arange(len(queries))

  width = 0.2
  algs_offset = {"WCOJ": 0.5 * -width, "NoMat": 0.5 * width}
  algs_labels = {"WCOJ": "LFTJ", "NoMat": "GraphWCOJ"}

  for a in algs:
    ax.bar(x + algs_offset[a], wcoj_times[a], align="center", width=width, label=algs_labels[a])

  plt.legend()
  plt.xticks(x, queries, rotation=45)
  plt.ylabel("Runtime time [s]")

  plt.grid(axis="y")

  plt.tight_layout()

  if OUTPUT:
    plt.savefig(FIGURE_PATH + output_name)

  plt.show()


data = read_dataset(DATASET_FOLDER + "final/sequential/snb-wcoj-graphwcoj.csv")
data = data.append(read_dataset(DATASET_FOLDER + "final/sequential/nomat-snb.csv", no_mat=True))
display_data(data, ["3-clique", "4-clique", "5-clique", "kite"], "wcoj-csr.svg")
