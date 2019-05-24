import pandas as pd
import matplotlib.pyplot as plt

from diagrams.base import *

DATASET = DATASET_FOLDER + "ama0601.csv"

data = pd.read_csv(DATASET)

data = data[data["Query"] != "Cycle(5)"]
data = data[data["Query"] != "Cycle(6)"]

data = data.set_index("Query")

fix_count(data)


wcojData = data[data["Algorithm"] == "WCOJ"][["WCOJTime"]]
binData = data[data["Algorithm"] == "sbin"][["Time"]]

displayData = binData.join(wcojData, "Query")
displayData.rename(columns={"WCOJTime": "\\texttt{seq}", "Time": "\\texttt{BroadcastHashJoin}"}, inplace=True)


a = displayData.plot.bar()
autolabel(a.patches, "right")

plt.xlabel("")
plt.ylabel("Time [s]")
plt.xticks(rotation=0)

axes = plt.gca()
# axes.set_ylim([0, 50])
plt.grid(axis="x")

plt.tight_layout()

plt.savefig(FIGURE_PATH + "seq-bar-ama0601.svg")
plt.show()

# TODO rearrange clique 5 labelling