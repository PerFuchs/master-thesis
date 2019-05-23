import pandas as pd
import matplotlib.pyplot as plt

from diagrams.base import *

DATASET = DATASET_FOLDER + "ama0302.csv"

def tabulize_data(data_path, output_path):
  data = pd.read_csv(data_path)

  fix_count(data)
  fix_neg(data, "copy")


  wcojData = data[data["Algorithm"] == "WCOJ"]
  binData = data[data["Algorithm"] == "sbin"]
  data = binData.merge(wcojData, on=["Query"], suffixes=("", "_wcoj"))
  data["ratio"] = data["Time"] / data["WCOJTime_wcoj"]
  data["setup"] = data["mat_wcoj"] + data["copy_wcoj"]  # TODO misses sorting

  data.to_latex(buf=open(output_path, "w"),
                columns=["Query", "Count", "Time", "WCOJTime_wcoj", "setup", "ratio"],
                header = ["Query", "\\# Result", "\\texttt{BroadcastHashJoin}", "\\texttt{seq}", "setup", "Speedup"],
                column_format="lr||r|rr||r",
                formatters = {
                  "ratio": lambda r: str(round(r, 1)),
                  "Count": lambda c: "{:,}".format(c),
                },
                escape=False,
                index=False
                )


tabulize_data(DATASET_FOLDER + "ama0302.csv", GENERATED_PATH + "seq-table-ama0302.tex")
tabulize_data(DATASET_FOLDER + "ama0601.csv", GENERATED_PATH + "seq-table-ama0601.tex")
tabulize_data(DATASET_FOLDER + "snb-sf1.csv", GENERATED_PATH + "seq-table-snb-sf1.tex")
