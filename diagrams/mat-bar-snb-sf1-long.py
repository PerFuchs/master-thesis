import pandas as pd
import copy

from diagrams.base import *

DATASET = DATASET_FOLDER + "snb-sf1-mat.csv"

data = pd.read_csv(DATASET, sep=",", comment="#")

data = data[data["Query"] != "kite"]
data = data[data["Query"] != "3-clique"]
data = data[data["Query"] != "4-clique"]
data = data[data["Query"] != "5-clique"]

data = data.set_index("Query")

fix_count(data)

# data = data[["WCOJTime"]]

graphWCOJData = data[data["Algorithm"] == "GraphWCOJ"][["WCOJTime"]]
noMat = data[data["Algorithm"] == "NoMat"][["WCOJTime"]]

joined = noMat.join(graphWCOJData, "Query", lsuffix="_seq")
joined.rename(columns={"WCOJTime_seq": "\\texttt{No Mat}", "WCOJTime": "\\texttt{mat}"}, inplace=True)
joined = joined.groupby("Query")

means = joined.median().round(2)
qo = copy.copy(QUERY_ORDER)
qo.remove("4-clique")
qo.remove("3-clique")
qo.remove("kite")
# qo.remove("4-cycle")
qo.remove("5-cycle")
qo.remove("3-0.00-path")  # TODO rename path query to 3-0.01-path
qo.remove("5-clique")
# qo.remove("house")
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

plt.savefig(FIGURE_PATH + "mat-graph-bar-snb-sf1-long.svg")
plt.show()