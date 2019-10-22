import os
import pandas as pd
import copy
import numpy as np

from diagrams.base import *

dataset_location = DATASET_FOLDER + "final/linear-search/"
THRESHOLDS_SNB = [1, 50, 100, 200, 400, 800, 1600]


def read_dataset_and_tag(dataset_path, threshold):
  data = pd.read_csv(dataset_path, sep=",", comment="#")
  # fix_count(data)

  data["threshold"] = threshold
  return data


def get_data_for_dataset(dataset_name, thresholds):
  dataset_name = dataset_name + "-"
  if dataset_name == "snb-1-":
    dataset_name = ""

  data = read_dataset_and_tag(dataset_location + "linsearch-%s1.csv" % dataset_name, 1)
  data = data.append(read_dataset_and_tag(dataset_location + "linsearch-lftj-%s1.csv" % dataset_name, 1))

  ts = copy.copy(thresholds)
  ts.remove(1)

  for a in ["lftj-", ""]:
    for threshold in ts:
      file_path = dataset_location + "linsearch-%s%s%i.csv" % (a, dataset_name, threshold)
      if os.path.isfile(file_path):
        data = data.append(read_dataset_and_tag(file_path, threshold))

  data["wcoj_time"] = (data["AlgoEnd-0"] - data["Scheduled-0"]) / 1000
  data = data.groupby(["Algorithm", "threshold"])
  data = data.median().round(2)

  return data


def bar_chart(datasetname, data, thresholds):
  x = list(range(len(thresholds)))

  mediansGraphWCOJ = data["wcoj_time"]["GraphWCOJ"]
  mediansLFTJ = data["wcoj_time"]["WCOJ"]

  width = 0.3

  fig, ax = plt.subplots()

  ax.bar(x, mediansGraphWCOJ, align="edge", width=width, label="GraphWCOJ")
  ax.bar(x, mediansLFTJ,  align="edge", width=-width, label="LFTJ")

  ax.legend()
  plt.xticks(x, thresholds)
  plt.xlabel("Threshold")
  plt.ylabel("WCOJ Time [s]")

  plt.tight_layout()

  plt.savefig(FIGURE_PATH + "linear-search-threshold-%s.svg" % datasetname)
  plt.show()


data = get_data_for_dataset("snb-1", THRESHOLDS_SNB)
bar_chart("snb", data, THRESHOLDS_SNB)
