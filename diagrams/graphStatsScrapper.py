from html.parser import HTMLParser
import requests
import diagrams.base

import numpy as np
import matplotlib.pyplot as plt

DATATYPE_SIZE = 32

class GraphData:
  def __init__(self, vertices, edges):
    self.vertices = vertices
    self.edges = edges

  def size_in_bits(self):
    return DATATYPE_SIZE  * self.vertices + DATATYPE_SIZE  * self.edges

  def size_in_gb(self):
    return self.size_in_bits() / 8e+9

def lwa_parse_number(number: str) -> int:
  number = number.replace('\u2009', '')
  number = number.replace(' ', '')
  return int(number)


def snap_parse_number(number: str) -> int:
  number = number.replace(',', '')
  number = number.replace(' ', '')
  number = number.replace('\r\n', '')
  number = number.replace('~', '')
  print(number)
  return int(number)

class LaboratoryWebAlgorithmicsParser(HTMLParser):
  VERTICE_COLUMN = 3
  EDGE_COLUMN = 4

  def __init__(self):
    super().__init__()
    self.in_body = False
    self.in_row = False
    self.column_counter = 0
    self.vertices = []
    self.edges = []

  def handle_starttag(self, startTag, attrs):
    if startTag == 'tbody':
      self.in_body = True
    elif startTag == 'tr':
      self.in_row = True
      self.column_counter = 0

    if self.in_body and self.in_row and startTag == 'td':
      self.column_counter += 1

  def handle_data(self, data: str) -> None:
    if self.in_body and self.in_row:
      if self.column_counter == self.VERTICE_COLUMN:
        self.vertices.append(lwa_parse_number(data))
      elif self.column_counter == self.EDGE_COLUMN:
        self.edges.append(lwa_parse_number(data))

  def handle_endtag(self, endTag):
    if endTag == 'tbody':
      self.in_body = False
    elif endTag == 'tr':
      self.in_row = False
      self.column_counter = 0


class StanfordGraphsParser(HTMLParser):
  VERTICE_COLUMN = 3
  EDGE_COLUMN = 4
  EXCLUDE_CATEGORIES = ['Online Reviews', 'Online Communities',
                        'Wikipedia networks, articles, and metadata',
                        'Autonomous systems graphs',
                        'Memetracker and Twitter', 'Temporal networks']

  def __init__(self):
    super().__init__()
    self.in_table = False
    self.in_row = False
    self.column_counter = 0
    self.row_counter = 0
    self.vertices = []
    self.edges = []
    self.in_h3 = False
    self.exclude_graphs = False

  def handle_starttag(self, startTag, attrs):
    if startTag == 'table':
      self.in_table = True
      self.row_counter = 0
    elif startTag == 'tr':
      self.in_row = True
      self.row_counter += 1
      self.column_counter = 0
    elif startTag == 'h3':
      self.exclude_graphs = False
      self.in_h3 = True

    if self.in_table and self.in_row and startTag == 'td':
      print("snap in column", self.column_counter)
      self.column_counter += 1

  def handle_data(self, data: str) -> None:
    data = data.strip()
    if self.in_table and self.in_row and self.row_counter != 1 and data != '' and not self.exclude_graphs:
      if self.column_counter == self.VERTICE_COLUMN:
        self.vertices.append(snap_parse_number(data))
      elif self.column_counter == self.EDGE_COLUMN:
        self.edges.append(snap_parse_number(data))
    elif self.in_h3 and data in self.EXCLUDE_CATEGORIES:
        print("Excluding ", data)
        self.exclude_graphs = True


  def handle_endtag(self, endTag):
      if endTag == 'table':
        self.in_table = False
      elif endTag == 'tr':
        self.in_row = False
        self.column_counter = 0
      elif endTag == 'h3':
        self.in_h3 = False


def scrap_laboratory_web_algorithmics():
  parser = LaboratoryWebAlgorithmicsParser()
  html_page = requests.get("http://law.di.unimi.it/datasets.php").text

  parser.feed(html_page)

  assert len(parser.vertices) == len(parser.edges)
  return list(map(lambda g: GraphData(g[0], g[1]), zip(parser.vertices, parser.edges)))


def scrap_stanford_graphs():
  parser = StanfordGraphsParser()
  html_page = requests.get("https://snap.stanford.edu/data/").text

  print(html_page)
  parser.feed(html_page)
  print(parser.vertices)

  assert len(parser.vertices) == len(parser.edges)
  return list(map(lambda g: GraphData(g[0], g[1]), zip(parser.vertices, parser.edges)))


def main():
  graphs = scrap_laboratory_web_algorithmics()
  graphs += scrap_stanford_graphs()

  graph_sizes = list(map(lambda g: g.size_in_gb(), graphs))
  print("Sizes: ", graph_sizes)
  print("Graph count: ", len(graphs))
  print("Less than 1GB ", len(list(filter(lambda s: s < 1, graph_sizes))))
  print("More than 1GB ", len(list(filter(lambda s: s > 1, graph_sizes))))
  print("Maximum: ", max(graph_sizes))

  bins = list(range(1, 100, 5)) + list(range(100, 650, 50))
  n, bins, patches = plt.hist(graph_sizes, bins=bins, histtype="bar")

  plt.xlabel('Size [GB]')
  plt.ylabel('\\# Graphs')
  # plt.text(60, .025, r'$\mu=100,\ \sigma=15$')
  # plt.axis([40, 160, 0, 0.03])
  plt.grid(True)
  plt.tight_layout()
  plt.savefig("../graph-sizes.svg")
  plt.show()


main()
