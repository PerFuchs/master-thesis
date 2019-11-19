import matplotlib.pyplot as plt
import pandas as pd

DATASET_FOLDER = "../data/"
FIGURE_PATH = "../svg/"
GENERATED_PATH= "../generated/"

WORKSTEALING = "FirstVariablePartitioningWithWorkstealing"
SHARES = "Shares"

partitioning_names = {
  WORKSTEALING: "work-stealing",
  SHARES: "Shares"
}

plt.rc('text', usetex=True)
plt.rc('font', family='serif', size=16)

QUERY_ORDER = ["3-0.00-path", "3-clique", "kite", "4-clique", "house", "5-clique", "4-cycle", "diamond", "5-cycle"]

def fix_count(data):
  data["Count"] = data["Count"].map(lambda x: int(x.replace(".", "")))


def fix_neg(data, col):
  data[col] = data[col].map(lambda v: abs(v))


def autolabel(rects, xpos='center', ints=False):
  """
  Attach a text label above each bar in *rects*, displaying its height.

  *xpos* indicates which side to place the text w.r.t. the center of
  the bar. It can be one of the following {'center', 'right', 'left'}.
  """

  ha = {'center': 'center', 'right': 'left', 'left': 'right'}
  offset = {'center': 0, 'right': 1, 'left': -1}
  ax = plt.gca()
  for rect in rects:
    height = rect.get_height()
    ax.annotate('%i' % height if ints else '%.2f' % height,
                xy=(rect.get_x() + rect.get_width() / 2, height),
                xytext=(offset[xpos]*3, 3),  # use 3 points offset
                textcoords="offset points",  # in both directions
                ha=ha[xpos], va='bottom')


def write_to_file(file_path, string):
  with open(file_path, "w") as f:
    f.write(string)


def fix_shares(file_path, new_path):
  lines = list()
  with open(file_path, "r") as f:
    for l in f:
      low = l.find("Shares(")
      new_line = l
      if low != -1:
        high = l.find(")", low)
        innerLow = l.find("(", low, high)
        replacement = l[innerLow: high].replace(",", ";")
        new_line = l[0:innerLow] + replacement + l[high:]
      lines.append(new_line)
  with open(new_path, "w") as f:
    for l in lines:
      f.write(l)


def split_partitioning(data):
  def split(p):
    s = p.split("(")
    if s[0] == "SharesRange":
      hc_conf = s[1]
      hc_conf = hc_conf.replace(")", "")
      hc_conf_parsed = list(map(lambda s: int(s), hc_conf.split(";")))
      if len(list(filter(lambda d: d != 1, hc_conf_parsed))) == 1:
        variable = list(map(lambda t: t[0] + 1, filter(lambda t: t[1] != 1, enumerate(hc_conf_parsed))))[0]
        s[0] = "%i-variable" % variable
    return s[0]
  data["partitioning_base"] = list(map(split, data["Partitioning"]))


def split_executor(data):
  def split(r):
    if r == "0":
      return None, None
    else:
      return r.split(":")

  def split_address(r):
    return split(r)[0]

  def split_thread(r):
    return split(r)[1]

  executor_cols = list(filter(lambda c: c.startswith("Executor"), data.columns))
  for c in executor_cols:
    number = c.replace("Executor-", "")
    data["executor-%s" % number] = list(map(split_address, data[c]))
    data["thread-%s" % number] = list(map(split_thread, data[c]))
  return data


def map_executor_to_numbers(data):
  executor_cols = list(filter(lambda c: c.startswith("executor"), data.columns))

  addresses = []
  for e in executor_cols:
    for a in data[e]:
      if a is not None:
        addresses.append(a)
  addresses = list(set(addresses))
  addresses.sort()

  def map_to_numbers(e):
    if e is not None:
      return addresses.index(e)

  for c in executor_cols:
    number = c.replace("executor-", "")
    data["executor-number-%s" % number] = list(map(map_to_numbers, data[c]))
  return data


def get_values(row, column_names):
  values = list(map(lambda n: row[n], column_names))
  return values


def add_wcoj_time(data):
  column_names = data.columns.values
  scheduled_columns = list(filter(lambda s: s.startswith("Scheduled"), column_names))
  ends_columns = list(filter(lambda s: s.startswith("AlgoEnd"), column_names))

  data["wcoj_time"] = data.apply(lambda r: max(get_values(r, ends_columns)) -
                                           min(list(filter(lambda  v: 0 < v, get_values(r, scheduled_columns)))),
                                                                                       axis=1)
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


def add_skew(data):
  data["skew"] = data["max-worker-time"] / data["min-worker-time"]
  return data


def add_spark_overhead(data):
  column_names = data.columns.values
  scheduled_columns = list(filter(lambda s: s.startswith("Scheduled"), column_names))
  ends_columns = list(filter(lambda s: s.startswith("AlgoEnd"), column_names))

  def applyFunc(r):
    first_start = min(list(filter(lambda  v: 0 < v, get_values(r, scheduled_columns))))
    last_end = max(get_values(r, ends_columns))

    return ((r["End"] - r["Start"]) - (last_end - first_start))
  data["spark_overhead"] = data.apply(applyFunc, axis=1)
  return data

def fix_missing_columns(input_file_path, output_file_path, parallelism):
  data = pd.read_csv(input_file_path, sep=",", comment="#")
  existing_columns = data.columns.values

  parallelism_dependend_column_prefixes = ["WCOJTime-", "Scheduled-", "AlgoEnd-"]

  new_columns = []
  for column_prefix in parallelism_dependend_column_prefixes:
    for p in range(0, parallelism):
      column_name = column_prefix + str(p)
      if not column_name in existing_columns:
        default_value = 0
        if column_prefix == "WCOJTime-":
          default_value = 0.0
        data[column_name] = default_value
        new_columns.append(column_name)

  new_column_order = []
  for i, column_name in enumerate(existing_columns):
    new_column_order.append(column_name)
    for column_prefix in parallelism_dependend_column_prefixes:
      if column_name.startswith(column_prefix) and \
        (i + 1 == len(existing_columns) or not existing_columns[i + 1].startswith(column_prefix)):
        for new_column in new_columns:
          if new_column.startswith(column_prefix):
            new_column_order.append(new_column)

  data.to_csv(output_file_path, sep=",", columns=new_column_order, index=False)

