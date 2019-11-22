from collections import defaultdict, OrderedDict
from pandas import DataFrame

from diagrams.base import *


def read_dataset(dataset_path):
  data = pd.read_csv(dataset_path, sep=",", comment="#")
  fix_count(data)
  split_partitioning(data)
  add_wcoj_time(data)
  split_executor(data)
  map_executor_to_numbers(data)
  add_end_time_difference(data)

  data["total"] = (data["End"] - data["Start"])
  return data


def add_end_time_difference(data):
  column_names = data.columns.values
  end_columns = list(filter(lambda s: s.startswith("AlgoEnd"), column_names))

  def applyFunc(r):
    ends = get_values(r, end_columns)
    ends = list(filter(lambda v: v != 0, ends))

    return max(ends) - min(ends)

  def max_index(r):
    ends = get_values(r, end_columns)
    ends = list(filter(lambda v: v != 0, ends))

    max_v = max(ends)
    max_i = ends.index(max_v)
    return max_i

  def Max(r):
    ends = get_values(r, end_columns)
    ends = list(filter(lambda v: v != 0, ends))
    return max(ends)

  def Min(r):
    ends = get_values(r, end_columns)
    ends = list(filter(lambda v: v != 0, ends))
    return min(ends)

  data["difference"] = data.apply(applyFunc, axis=1)
  data["max_end"] = data.apply(Max, axis=1)
  data["min_end"] = data.apply(Min, axis=1)
  data["longest"] = data.apply(max_index, axis=1)
  data["skew"] = data["difference"] / data["wcoj_time"]
  return data


def assert_work_time_differences_small(work_time_differences):
  for v in work_time_differences.values():
    minimum = min(v)
    plus_5 = minimum + minimum * 0.05
    # print(v)
    if plus_5 < 50:
      plus_5 = 500
    for d in v:
      assert(plus_5 >= d)


def calculate_average_workers_end(data, parallelism):
  f = data[data["Parallelism"] == parallelism]

  keys = f.keys().get_values()
  averages = []
  for t in f.itertuples():
    r = dict(zip(keys, t[1:]))

    end_time_differences = defaultdict(lambda: list())

    parallelism = r["Parallelism"]
    for p in range(parallelism):
      end_time_differences[r["executor-number-%i" % p]].append(r["AlgoEnd-%i" % p] - r["min_end"])
    # if (parallelism == 96):
    #   print(end_time_differences)

    # pprint.pprint(end_time_differences)
    # assert_work_time_differences_small(end_time_differences)

    average = OrderedDict()
    for k, v in end_time_differences.items():
      average[k] = sum(v) / len(v)
    averages.append(average)

  # print(averages)
  total_average = defaultdict(lambda: list())
  for a in averages:
    for k, v in a.items():
      total_average[k].append(v)

  return dict(map(lambda t: (t[0], sum(t[1]) / len(t[1])), total_average.items()) )


def calculate_worker_skew(averages):
  max_worker = max(averages.values())
  average_worker = sum(averages.values()) / len(averages.values())
  mininmal_worker = min(averages.values())
  return max_worker - average_worker


def table_for_dataset(dataset_path, output_file_name, filter_batched=True, filter_normal=False):
  data = read_dataset(dataset_path)

  g = data.groupby(["Query", "Parallelism"])
  m = g.median()

  parallelism = list(set(data["Parallelism"]))
  queries = list(set(data["Query"]))
  queries.sort()
  parallelism.sort()

  partitionings = list(set(data["Partitioning"]))
  partitionings.sort()

  rows = defaultdict(lambda: list())
  for pa in partitionings:
    d = data[data["Partitioning"] == pa]
    for q in queries:
      f = d[d["Query"] == q]
      if f.empty:
        continue

      query = q
      if q == "3-clique":
        if pa == WORKSTEALING:
          query += " batch 1"
        else:
          query += " batch 40"
      rows["Query"].append(query)
      worker_skew = OrderedDict()
      for p in parallelism:
        worker_skew[p] = calculate_worker_skew(calculate_average_workers_end(f, p))

      worker_skew_ratio = OrderedDict()
      for p in parallelism:
        worker_skew_ratio[p] = worker_skew[p] / m["total"][q][p]

      for p in parallelism:
        rows["skew-%i" % p].append("%.2f / %.1f" % (worker_skew[p] / 1000, worker_skew_ratio[p] * 100))


  table_frame = DataFrame(rows)
  table_frame = table_frame.sort_values("Query")

  table_frame.to_latex(buf=open(output_file_name, "w"),
                       columns=["Query"] + list(map(lambda p: "skew-%i" % p, parallelism)),
                       header=["Query"] + list(map(lambda p: "%i [s] / [%%]" % p if p == 16 else "%i" % p, parallelism)),
                       index=False)
  return data

table_for_dataset(DATASET_FOLDER + "final/distributed/orkut.csv", GENERATED_PATH + "distributed-skew-orkut.tex")
data = table_for_dataset(DATASET_FOLDER + "final/distributed/liveJ.csv", GENERATED_PATH + "distributed-skew-liveJ.tex", False, True)




