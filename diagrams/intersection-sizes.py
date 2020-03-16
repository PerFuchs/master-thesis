import pandas as pd
import copy

from diagrams.base import *

DATASET = DATASET_FOLDER + "intersection-sizes-5-clique-snb-sf1.csv"

data = pd.read_csv(DATASET, sep=",", comment="#")

data["difference"] = data["smallestIteratorBiggest"] - data["total"]

print("Total max: ", max(data["total"]))
print("Smallest max: ", max(data["smallestIterator"]))
print("Smallest with any (biggest) max: ", max(data["smallestIteratorBiggest"]))


project = data[["total"]]
a = project.plot.hist(histtype="stepfilled", bins=list(range(0, 200, 1)), density=True, cumulative=True)

a.legend().remove()
plt.xlabel("Size")
plt.ylabel("CDF")

plt.tight_layout()
plt.savefig(FIGURE_PATH + "/intersections/total.svg")
plt.show()

project = data[["smallestIterator"]]
a = project.plot.hist(histtype="stepfilled", bins=list(range(0, 200, 1)), density=True, cumulative=True)

a.legend().remove()
plt.xlabel("Size")
plt.ylabel("CDF")

plt.tight_layout()
plt.savefig(FIGURE_PATH + "/intersections/smallest.svg")
plt.show()


project = data[["smallestIteratorBiggest"]]
a = project.plot.hist(histtype="stepfilled", bins=list(range(0, 200, 1)), density=True, cumulative=True)

a.legend().remove()
plt.xlabel("Size")
plt.ylabel("CDF")

plt.tight_layout()
plt.savefig(FIGURE_PATH + "/intersections/smallest-biggest.svg")
plt.show()
