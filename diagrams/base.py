import matplotlib.pyplot as plt

DATASET_FOLDER = "../data/"
FIGURE_PATH = "../"

plt.style.use("fivethirtyeight")
plt.rc('text', usetex=True)
plt.rc('font', family='serif')


def fix_count(data):
  data["Count"] = data["Count"].map(lambda x: int(x.replace(".", "")))


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
    ax.annotate('{}'.format(height),
                xy=(rect.get_x() + rect.get_width() / 2, height),
                xytext=(offset[xpos]*3, 3),  # use 3 points offset
                textcoords="offset points",  # in both directions
                ha=ha[xpos], va='bottom')