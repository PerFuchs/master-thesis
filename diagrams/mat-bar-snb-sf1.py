import pandas as pd
import copy

from diagrams.base import *

DATASET = DATASET_FOLDER + "snb-sf1-mat.csv"

data = pd.read_csv(DATASET, sep=",", comment="#")

data = data[data["Query"] != "4-cycle"]
data = data[data["Query"] != "house"]

data = data.set_index("Query")

fix_count(data)

# data = data[["WCOJTime"]]

graphWCOJData = data[data["Algorithm"] == "GraphWCOJ"][["WCOJTime"]]
noMat = data[data["Algorithm"] == "NoMat"][["WCOJTime"]]

joined = noMat.join(graphWCOJData, "Query", lsuffix="_seq")
joined.rename(columns={"WCOJTime_seq": "Leapfrog join", "WCOJTime": "Materializing LF"}, inplace=True)
joined = joined.groupby("Query")

means = joined.median().round(2)
qo = copy.copy(QUERY_ORDER)
qo.remove("3-0.00-path")
qo.remove("4-cycle")
qo.remove("5-cycle")
qo.remove("house")
qo.remove("diamond")
means = means.reindex(qo)
errors = joined.std()
errors = errors.reindex(qo)

a = means.plot.bar(yerr=errors, capsize=5)
autolabel(a.patches, "right")

plt.xlabel("")
plt.ylabel("Time [s]")
plt.xticks(rotation=45)

axes = plt.gca()
plt.grid(axis="x")

plt.tight_layout()

plt.savefig(FIGURE_PATH + "mat-graph-bar-snb-sf1.svg")
plt.show()