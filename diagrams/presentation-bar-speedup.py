import pandas as pd
import copy
import matplotlib as mp
from diagrams.base import *

DATASET = DATASET_FOLDER + "snb-sf1-presentation.csv"

data = pd.read_csv(DATASET, sep=",", comment="#")

# data = data[data["Query"] != "4-cycle"]
# data = data[data["Query"] != "house"]

data = data.set_index("Query")

fix_count(data)

# data = data[["WCOJTime"]]

spark = data[data["Algorithm"] == "BinaryJoins"][["Time"]]
graphWCOJData = data[data["Algorithm"] == "GraphWCOJ"][["WCOJTime"]]
wcoj = data[data["Algorithm"] == "WCOJ"][["WCOJTime"]]

joined = spark.join(wcoj, "Query", lsuffix="_seq")
joined = joined.join(graphWCOJData, "Query", rsuffix="_graph")
joined["speedup_spark"] = 1
joined["speedup_wcoj"] = joined["Time"] / joined["WCOJTime"]


joined["speedup_wcoj_graph"] = joined["Time"] / joined["WCOJTime_graph"]
joined = joined.drop(columns="Time")
joined = joined.drop(columns="WCOJTime")
joined = joined.drop(columns="WCOJTime_graph")

# diff = joined
#
# diff["difference"] = diff["speedup_wcoj_graph"] - diff["speedup_wcoj"]
# print("Difference max", diff["difference"].max())


joined.rename(columns={"speedup_spark": "\\texttt{Spark}", "speedup_wcoj": "\\texttt{WCOJ}", "speedup_wcoj_graph": "\\texttt{GraphWCOJ}"},
              inplace=True)
joined = joined.groupby("Query")

means = joined.median().round(2)
qo = copy.copy(QUERY_ORDER)
qo.remove("3-0.00-path")
# qo.remove("diamond")
qo.remove("5-cycle")
# qo.remove("4-cycle")
# qo.remove("house")
means = means.reindex(qo)
# errors = joined.std()
# errors = errors.reindex(qo)

a = means.plot.bar(capsize=5)
# autolabel(a.patches, "right")

plt.xlabel("")
# plt.ylim(0, 40)
plt.ylabel("Speedup", size=25)
plt.xticks(size=25)
plt.yticks(size=25)
plt.xticks(rotation=45)

axes = plt.gca()
plt.grid(axis="x")

plt.tight_layout()

plt.savefig(FIGURE_PATH + "results-1-speedup.svg")
plt.show()