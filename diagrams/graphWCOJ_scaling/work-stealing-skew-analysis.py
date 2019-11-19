from collections import defaultdict
from pandas import DataFrame

from diagrams.base import *


def format_time(data, column_name):
  data[column_name + "_f"] = (data[column_name] / 1000).round(2)
  return data


def add_highest_scheduling_difference(data):
  column_names = data.columns.values
  scheduled_columns = list(filter(lambda s: s.startswith("Scheduled"), column_names))

  def applyFunc(r):
    scheduled = get_values(r, scheduled_columns)
    scheduled = list(filter(lambda v: v != 0, scheduled))

    return max(scheduled) - min(scheduled)

  data["scheduling_delay"] = data.apply(applyFunc, axis=1)
  return data


def add_end_time_difference(data):
  column_names = data.columns.values
  end_columns = list(filter(lambda s: s.startswith("AlgoEnd"), column_names))

  def Max(r):
    ends = get_values(r, end_columns)
    ends = list(filter(lambda v: v != 0, ends))
    return max(ends)

  def Min(r):
    ends = get_values(r, end_columns)
    ends = list(filter(lambda v: v != 0, ends))
    return min(ends)

  def average(r):
    ends = get_values(r, end_columns)
    ends = list(filter(lambda v: v != 0, ends))
    return sum(ends) / len(ends)

  data["max_end"] = data.apply(Max, axis=1)
  data["min_end"] = data.apply(Min, axis=1)
  data["avg_end"] = data.apply(average, axis=1)
  data["skew"] = data["max_end"] - data["avg_end"]
  data["skew-rel"] = data["skew"] / data["wcoj_time"]
  return data


def add_taks_skew(data):
  column_names = data.columns.values
  tasks_columns = list(filter(lambda s: s.startswith("Tasks"), column_names))

  def applyFunc(r):
    ends = get_values(r, tasks_columns)
    ends = list(filter(lambda v: v != 0, ends))

    return max(ends) / min(ends)

  def max_index(r):
    ends = get_values(r, tasks_columns)
    ends = list(filter(lambda v: v != 0, ends))

    max_v = max(ends)
    max_i = ends.index(max_v)
    return max_i

  def Max(r):
    ends = get_values(r, tasks_columns)
    ends = list(filter(lambda v: v != 0, ends))
    return max(ends)

  def Min(r):
    ends = get_values(r, tasks_columns)
    ends = list(filter(lambda v: v != 0, ends))
    return min(ends)

  data["task_skew"] = data.apply(applyFunc, axis=1)
  data["most_tasks"] = data.apply(max_index, axis=1)
  data["max_tasks"] = data.apply(Max, axis=1)
  data["min_tasks"] = data.apply(Min, axis=1)

  return data


def read_data(csv_file):
  data = pd.read_csv(csv_file, sep=",", comment="#")

  fix_count(data)
  split_partitioning(data)

  data = data[data["partitioning_base"] == WORKSTEALING]

  data = add_wcoj_time(data)
  data = add_end_time_difference(data)
  return data


def output_table(output_filename, data):
  grouped = data.groupby(["Query", "Parallelism"])
  median = grouped.median()

  parallelism = [16, 32, 48, 64, 96]
  queries = ["3-clique", "4-clique", "5-clique"]

  columns = ["Query"]
  for p in parallelism:
    columns.append("skew-%i" % p)

  rows = defaultdict(lambda: list())
  for q in queries:
    rows["Query"].append(q)
    for p in parallelism:
      rows["skew-%i" % p].append("%.1f / %.2f" % (median["skew"][q][p] / 1000, median["skew-rel"][q][p] * 100))

  table_frame = DataFrame(rows)

  table_frame.to_latex(buf=open(output_filename, "w"),
                       columns=columns, header=["Query"] + list(map(lambda p: "%i [s] / [%%]" % p, parallelism)),
                       index=False)


DATASET_LIVEJ = DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-scaling.csv"
DATASET_ORKUT = DATASET_FOLDER + "final/graphWCOJ-scaling/orkut-scaling.csv"

data = read_data(DATASET_ORKUT)
output_table(GENERATED_PATH + "skew-orkut.tex", data)

data = read_data(DATASET_LIVEJ)
output_table(GENERATED_PATH + "skew-liveJ.tex", data)


# for i in range(0, 96):
#   data = format_time(data, "worker-time-" + str(i))
#
# worker_times = data[data.columns.intersection(list(filter(lambda n: n.startswith("worker-time-") and n.endswith("_f"),
#                                                           data.columns.values)))]
# worker_times.to_csv("worker-times.csv")

# p = 48
# f = data[data["Query"] == "5-clique"]
# f = f[f["Parallelism"] == p]
# f = f[f["partitioning_base"] == WORKSTEALING]
#
# x = list(range(p))
# keys = f.keys().get_values()
# for r1 in f.itertuples():
#   print("row")
#   r = dict(zip(keys, r1[1:]))
#   duration = r["max_end"] - r["min_end"]
#   if (duration != 0):
#   # tasks_range = r["max_tasks"] - r["min_tasks"]
#     print(duration)
#     # print(tasks_range)
#     time_ratios = []
#     # task_ratios = []
#     for w in range(p):
#       time_ratios.append(float(r["AlgoEnd-%i" % w] - r["min_end"]) / duration)
#       # task_ratios.append(float(r["Tasks-%i" % w] - r["min_tasks"]) / tasks_range)
#
#     # ratios = zip(time_ratios, task_ratios)
#     ratios = sorted(time_ratios)
#     plt.scatter(x, ratios, marker="d")
#     # plt.scatter(x, list(map(lambda t: t[1], ratios)), marker="o")
#
#
# plt.tight_layout()
# plt.show()