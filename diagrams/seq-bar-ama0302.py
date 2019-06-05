import pandas as pd
import matplotlib.pyplot as plt

from diagrams.base import *

DATASET = DATASET_FOLDER + "ama0302_1.csv"

data = pd.read_csv(DATASET, sep=",", comment="#")

data = data[data["Query"] != "Cycle(6)"]

data = data.set_index("Query")

fix_count(data)

wcojData = data[data["Algorithm"] == "WCOJ"][["WCOJTime"]]
binData = data[data["Algorithm"] == "BinaryJoins"][["Time"]]

displayData = binData.join(wcojData, "Query")
displayData.rename(columns={"WCOJTime": "\\texttt{seq}", "Time": "\\texttt{BroadcastHashJoin}"}, inplace=True)

grouped = displayData.groupby("Query")

median = grouped.median()
median = median.reindex(QUERY_ORDER)
errors = grouped.std()
errors = errors.reindex(QUERY_ORDER)

a = median.plot.bar(yerr=errors, capsize=5)
autolabel(a.patches, "right")

plt.xlabel("")
plt.ylabel("Time [s]")
plt.xticks(rotation=45)

axes = plt.gca()
plt.grid(axis="x")

plt.tight_layout()

plt.savefig(FIGURE_PATH + "seq-bar-ama0302.svg")
plt.show()