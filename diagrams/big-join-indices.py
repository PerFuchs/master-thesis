from collections import defaultdict
from scipy.stats import binom
import matplotlib.pyplot as plt

from diagrams.base import FIGURE_PATH

vs = list(range(2, 9))

ws = [1, 2, 4, 8, 16, 32, 64, 128]

indices = defaultdict(lambda: list())
for i, v in enumerate(vs):
  for w in ws:
    indices[i].append(1 - binom.pmf(0, v, 1 / w))

x = list(range(len(ws)))

f, ax = plt.subplots()

for i, v in enumerate(vs):
  ax.plot(x, indices[i], '-o', label=str(v) + " variables")

plt.legend()
plt.xticks(x, ws)
plt.xlabel("# workers")
plt.ylabel("split of indices on each worker [\%]")

plt.savefig(FIGURE_PATH + "big-join-indices.svg")
plt.show()
