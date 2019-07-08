import os
import pandas as pd
import copy

from os import listdir

from diagrams.base import *

DATASETS = DATASET_FOLDER + "shares-simulation/"
SAMPLESIZE = 10000

def loadDatasets(folder):
  datasets = []
  for dir in listdir(folder):
    workerDir = "/".join((folder, dir))
    if os.path.isdir(workerDir):
      workers = int(dir)
      for q in listdir(workerDir):
        queryDir = "/".join((workerDir, q))
        if os.path.isdir(queryDir):
          query = q
          for f in listdir(queryDir):
            if f.endswith(".csv"):
              data = pd.read_csv("/".join((queryDir, f)), sep=",", header=None, names=("worker", "count"))
              data["query"] = query
              data["workers"] = workers
              datasets.append(data)

  concat = datasets[0]
  for d in datasets[1:]:
    concat = concat.append(d)
  return concat


data = loadDatasets(DATASETS)
data[["count"]] = data[["count"]] / SAMPLESIZE  # Express count as percentage

data = data.set_index("query")

workers128 = data[data["workers"] == 128][["count"]]
workers64 = data[data["workers"] == 64][["count"]]

joined = workers64.join(workers128, "query", lsuffix="_64")
# joined = workers64
grouped = joined.groupby("query")
means = grouped.mean()
means.count_64 = means[["count_64"]] * 100
means[["count"]] = means[["count"]] * 100
means = means.round()
ma = grouped.max().round(2)
mi = grouped.min().round(2)

means = means.sort_values("count_64")

means = means.drop(columns="count")

# means.rename(columns={"count_64": "64 workers", "count": "128 workers"}, inplace=True)
means.rename(columns={"count_64": "64 workers"}, inplace=True)
a = means.plot.bar(yerr=mi)
a.legend().remove()
# autolabel(a.patches, "right", True)
#
plt.xlabel("")
plt.ylabel("tuples per worker [\\%]", size=25)
plt.ylim(0, 100)
plt.xticks(rotation=45, size=25)
plt.yticks(size=25)
#
# axes = plt.gca()
# plt.grid(axis="x")
#
plt.tight_layout()
#
plt.savefig(FIGURE_PATH + "shares-bar.svg")
plt.show()