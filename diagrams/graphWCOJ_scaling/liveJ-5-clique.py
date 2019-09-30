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


def add_skew(data):
  column_names = data.columns.values
  end_columns = list(filter(lambda s: s.startswith("AlgoEnd"), column_names))

  def applyFunc(r):
    ends = get_values(r, end_columns)
    ends = list(filter(lambda v: v != 0, ends))

    return max(ends) - min(ends)

  data["skew"] = data.apply(applyFunc, axis=1)
  return data


def add_worker_times(data):
  column_names = data.columns.values

  for c in column_names:
    if c.startswith("Scheduled"):
      otherName = c.replace("Scheduled", "AlgoEnd")
      p = c.replace("Scheduled-", "")
      data["worker-time-" + p] = data[otherName] - data[c]

  column_names = data.columns.values
  worker_time_names = list(filter(lambda c: c.startswith("worker-time"), column_names))

  def min_worker_times(r):
    return min(list(filter(lambda v: v != 0, get_values(r, worker_time_names))))

  def max_worker_times(r):
    return max(list(get_values(r, worker_time_names)))

  data["max-worker-time"] = data.apply(max_worker_times, axis=1)
  data["min-worker-time"] = data.apply(min_worker_times, axis=1)

  return data


DATASET_LIVEJ = "../" + DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-scaling.csv"

data = pd.read_csv(DATASET_LIVEJ, sep=",", comment="#")

fix_count(data)
split_partitioning(data)

data = data[data["Query"] == "5-clique"]
data = data[data["partitioning_base"] == WORKSTEALING]

data = add_wcoj_time(data)
data = add_spark_overhead(data)
data = add_skew(data)
data = add_highest_scheduling_difference(data)
data = add_worker_times(data)

data = format_time(data, "scheduling_delay")
data = format_time(data, "spark_overhead")
data = format_time(data, "wcoj_time")
data = format_time(data, "skew")
data = format_time(data, "min-worker-time")
data = format_time(data, "max-worker-time")

for i in range(0, 96):
  data = format_time(data, "worker-time-" + str(i))

worker_times = data[data.columns.intersection(list(filter(lambda n: n.startswith("worker-time-") and n.endswith("_f"),
                                                          data.columns.values)))]
worker_times.to_csv("worker-times.csv")