import pandas as pd
import copy

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
joined.rename(columns={"Time": "\\texttt{Spark}", "WCOJTime": "\\texttt{WCOJ}", "WCOJTime_graph": "\\texttt{GraphWCOJ}"}, inplace=True)
joined = joined.groupby("Query")

means = joined.median().round(2)
qo = copy.copy(QUERY_ORDER)
qo.remove("3-0.00-path")
qo.remove("3-clique")
qo.remove("5-cycle")
qo.remove("4-clique")
qo.remove("5-clique")
qo.remove("kite")
means = means.reindex(qo)
errors = joined.std()
errors = errors.reindex(qo)

a = means.plot.bar(yerr=errors, capsize=5)
# autolabel(a.patches, "right")

plt.xlabel("")
# plt.ylim(0, 40)
plt.ylabel("Time [s]")
plt.xticks(rotation=45)

axes = plt.gca()
plt.grid(axis="x")

plt.tight_layout()

plt.savefig(FIGURE_PATH + "results-2.svg")
plt.show()