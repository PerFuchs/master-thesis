import pandas as pd
import copy
import matplotlib as mp
from diagrams.base import *

DATASET = DATASET_FOLDER + "no-mat-graph-wcoj.csv"

data = pd.read_csv(DATASET, sep=",", comment="#")

data = data[data["Query"] != "house"]

data = data.set_index("Query")

fix_count(data)

# data = data[["WCOJTime"]]

spark = data[data["Algorithm"] == "BinaryJoins"][["Time"]]
graphWCOJData = data[data["Algorithm"] == "GraphWCOJ"][["WCOJTime"]]
wcoj = data[data["Algorithm"] == "WCOJ"][["WCOJTime"]]

joined = spark.join(wcoj, "Query", lsuffix="_seq")
joined = joined.join(graphWCOJData, "Query", rsuffix="_graph")

joined = joined.drop(columns="Time")
joined.rename(columns={
  "WCOJTime": "WCOJ",
  "WCOJTime_graph": "GraphWCOJ"},
  inplace=True)


joined = joined.groupby("Query")

means = joined.median().round(2)
errors = joined.std()


qo = copy.copy(QUERY_ORDER)
qo.remove("3-0.00-path")
qo.remove("diamond")
qo.remove("5-cycle")
qo.remove("4-cycle")
qo.remove("house")
means = means.reindex(qo)
errors = errors.reindex(qo)

speedup = means["WCOJ"] / means["GraphWCOJ"]
print("Max speedup: ", max(speedup))

a = means.plot.bar(capsize=5, yerr=errors)
a.legend(loc="upper left")
# autolabel(a.patches, "right")

plt.xlabel("")
plt.ylabel("Time")
plt.xticks(rotation=45)

axes = plt.gca()
plt.grid(axis="x")

plt.tight_layout()

plt.savefig(FIGURE_PATH + "wcoj-vs-graph-wcoj.png")
plt.show()