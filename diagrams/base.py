import matplotlib.pyplot as plt
import pandas as pd

DATASET_FOLDER = "../data/"
FIGURE_PATH = "../"
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
    return p.split("(")[0]
  data["partitioning_base"] = list(map(split, data["Partitioning"]))


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
      print(column_name)
      if not column_name in existing_columns:
        print("Filling")
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