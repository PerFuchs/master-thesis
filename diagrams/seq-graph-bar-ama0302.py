import pandas as pd

from diagrams.base import *

DATASET = DATASET_FOLDER + "ama0302_1.csv"

data = pd.read_csv(DATASET, sep=",", comment="#")
data = data.set_index("Query")

fix_count(data)
# data = data[["WCOJTime"]]

graphWCOJData = data[data["Algorithm"] == "GraphWCOJ"][["WCOJTime"]]
wcojData = data[data["Algorithm"] == "WCOJ"][["WCOJTime"]]

joined = wcojData.join(graphWCOJData, "Query", lsuffix="_seq")
joined.rename(columns={"WCOJTime_seq": "\\texttt{seq}", "WCOJTime": "\\texttt{seq-graph}"}, inplace=True)
joined = joined.groupby("Query")

means = joined.median().round(2)
means = means.reindex(QUERY_ORDER)
errors = joined.std()
errors = errors.reindex(QUERY_ORDER)

a = means.plot.bar(yerr=errors, capsize=5)
autolabel(a.patches, "right")

plt.xlabel("")
plt.ylabel("Time [s]")
plt.xticks(rotation=45)

axes = plt.gca()
plt.grid(axis="x")

plt.tight_layout()

plt.savefig(FIGURE_PATH + "seq-graph-bar-ama0302.svg")
plt.show()