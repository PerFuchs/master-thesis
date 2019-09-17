import matplotlib.pyplot as plt

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

