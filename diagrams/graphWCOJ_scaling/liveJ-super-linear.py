import pandas as pd

from diagrams.base import *


def prepare_dataset(data):
  fix_count(data)
  split_partitioning(data)

  data["total_time"] = data["End"] - data["Start"]
  data = add_wcoj_time(data)
  return data


DATASET_LIVEJ_TASKSET_8 = DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-taskset-8.csv"
DATASET_LIVEJ_TASKSET_16 = DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-taskset-16.csv"
DATASET_LIVEJ_TASKSET_32 = DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-taskset-32.csv"

data_taskset_8 = pd.read_csv(DATASET_LIVEJ_TASKSET_8, sep=",", comment="#")
data_taskset = pd.read_csv(DATASET_LIVEJ_TASKSET_16, sep=",", comment="#")
data_taskset_32 = pd.read_csv(DATASET_LIVEJ_TASKSET_32, sep=",", comment="#")

data_taskset_8 = prepare_dataset(data_taskset_8)
data_taskset = prepare_dataset(data_taskset)
data_taskset_32 = prepare_dataset(data_taskset_32)

grouped_taskset_8 = data_taskset_8.groupby(["partitioning_base", "Query", "Parallelism"])
grouped_taskset = data_taskset.groupby(["partitioning_base", "Query", "Parallelism"])
grouped_taskset_32 = data_taskset_32.groupby(["partitioning_base", "Query", "Parallelism"])
queries = list(set(data_taskset["Query"]))

median_taskset_8 = grouped_taskset_8.median()
median_taskset = grouped_taskset.median()
median_taskset_32 = grouped_taskset_32.median()
scaling_8 = median_taskset_8["total_time"]["AllTuples"]["3-clique"][1] / median_taskset_8["total_time"][
  "FirstVariablePartitioningWithWorkstealing"][
  "3-clique"][8]

scaling_16 = median_taskset["total_time"]["AllTuples"]["3-clique"][1] / median_taskset["total_time"][
  "FirstVariablePartitioningWithWorkstealing"][
  "3-clique"][16]
scaling_32 = median_taskset_32["total_time"]["AllTuples"]["3-clique"][1] / median_taskset_32["total_time"][
  "FirstVariablePartitioningWithWorkstealing"][
  "3-clique"][32]

print("Scaling with 8 workers", scaling_8)
print("Scaling with 16 workers", scaling_16)
print("Scaling with 32 workers", scaling_32)
