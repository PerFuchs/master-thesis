import matplotlib.pyplot as plt

DATASET_FOLDER = "../data/"
FIGURE_PATH = "../"
GENERATED_PATH= "../generated/"

plt.style.use("fivethirtyeight")
plt.rc('text', usetex=True)
plt.rc('font', family='serif')


QUERY_ORDER = ["3-0.00-path", "3-clique", "kite", "4-clique", "house", "5-clique", "4-cycle", "5-cycle"]

def fix_count(data):
  data["Count"] = data["Count"].map(lambda x: int(x.replace(".", "")))


def fix_neg(data, col):
  data[col] = data[col].map(lambda v: abs(v))


def autolabel(rects, xpos='center'):
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
    ax.annotate('%.2f' % height,
                xy=(rect.get_x() + rect.get_width() / 2, height),
                xytext=(offset[xpos]*3, 3),  # use 3 points offset
                textcoords="offset points",  # in both directions
                ha=ha[xpos], va='bottom')


def write_to_file(file_path, string):
  with open(file_path, "w") as f:
    f.write(string)