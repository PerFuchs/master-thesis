import pandas as pd

from diagrams.base import *


def prepare_dataset(data):
  fix_count(data)
  split_partitioning(data)

  data["total_time"] = data["End"] - data["Start"]
  data = add_wcoj_time(data)
  return data


def read_data(input_path):
  data = pd.read_csv(input_path, sep=",", comment="#")
  prepare_dataset(data)
  return data


def get_scaling(data, data_baseline, parallelism):
  grouped = data.groupby(["partitioning_base", "Query", "Parallelism"])
  baseline_grouped = data_baseline.groupby(["partitioning_base", "Query", "Parallelism"])

  median = grouped.median()
  baseline_median = baseline_grouped.median()

  scaling = baseline_median["total_time"]["AllTuples"]["3-clique"][1] \
            / median["total_time"][WORKSTEALING if parallelism != 1 else "AllTuples"]["3-clique"][parallelism]

  return scaling

# Old taskset experiment
DATASET_LIVEJ_TASKSET_8 = DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-taskset-8.csv"
DATASET_LIVEJ_TASKSET_16 = DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-taskset-16.csv"
DATASET_LIVEJ_TASKSET_32 = DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-taskset-32.csv"

data_taskset_8 = read_data(DATASET_LIVEJ_TASKSET_8)
data_taskset = read_data(DATASET_LIVEJ_TASKSET_16)
data_taskset_32 = read_data(DATASET_LIVEJ_TASKSET_32)

print("Old taskset experiment")
print("Scaling with 8 workers", get_scaling(data_taskset_8, data_taskset_8, 8))
print("Scaling with 16 workers", get_scaling(data_taskset, data_taskset, 16))
print("Scaling with 32 workers", get_scaling(data_taskset_32, data_taskset_32, 32))


old_base = read_data(DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-scaling.csv")
turbo_base = read_data(DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-superlinear/turbo-base.csv")
no_turbo_base = read_data(DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-superlinear/no-turbo-base.csv")
no_turbo = read_data(DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-superlinear/lj-3c-no-turbo.csv")
no_turbo_taskset = read_data(DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-superlinear/lj-3c-taskset-no-turbo.csv")
turbo = read_data(DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-superlinear/lj-3c-turbo.csv")
turbo_taskset = read_data(DATASET_FOLDER + "final/graphWCOJ-scaling/liveJ-superlinear/lj-3c-turbo-taskset.csv")

print("Old taskset against original base")
print("Scaling with 8 workers", get_scaling(data_taskset_8, old_base, 8))
print("Scaling with 16 workers", get_scaling(data_taskset, old_base, 16))
print("Scaling with 32 workers", get_scaling(data_taskset_32, old_base, 32))

print("")
print("New taskset experiments")
print("Against old base")
print("no turbo", get_scaling(no_turbo, old_base, 16))
print("no turbo + taskset",  get_scaling(no_turbo_taskset, old_base, 16))
print("turbo",  get_scaling(turbo, old_base, 16))
print("turbo + taskset",  get_scaling(turbo_taskset, old_base, 16))

print("")
print("old against old")
print("Scaling with 8 workers", get_scaling(old_base, old_base, 8))
print("Scaling with 16 workers", get_scaling(old_base, old_base, 16))
print("Scaling with 32 workers", get_scaling(old_base, old_base, 32))


print("")
print("Against turbo base")
print("no turbo", get_scaling(no_turbo, turbo_base, 16))
print("no turbo + taskset",  get_scaling(no_turbo_taskset, turbo_base, 16))
print("turbo",  get_scaling(turbo, turbo_base, 16))
print("turbo + taskset",  get_scaling(turbo_taskset, turbo_base, 16))

print("Against no-turbo new base")
print("no turbo", get_scaling(no_turbo, no_turbo_base, 16))
print("no turbo + taskset",  get_scaling(no_turbo_taskset, no_turbo_base, 16))
print("turbo",  get_scaling(turbo, no_turbo_base, 16))
print("turbo + taskset",  get_scaling(turbo_taskset, no_turbo_base, 16))

print("Original against new no turbo base")
print("no turbo", get_scaling(no_turbo_base, old_base, 1))
print("taskset 16",  get_scaling(data_taskset, old_base, 1))
print("taskset 32",  get_scaling(data_taskset_32, old_base, 1))
print("taskset 8",  get_scaling(data_taskset_8, old_base, 1))
print("turbo",  get_scaling(turbo_base, old_base, 1))