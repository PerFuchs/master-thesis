val sparkSession = SparkSession.builder.
  master("local")
  .appName("WCOJ-spark")
  .getOrCreate()

// read a dataframe
val df = sparkSession.read
  .format("com.databricks.spark.csv")
  .option("inferSchema", "true")
  .load("/path/to/edge/relationship")

// df needs columns called `edge_id`, `src` and `dst`
val edges: CachedGraphTopologyFrame = df.cachedGraphTopology()

// Use WCOJ to find a triangle pattern
val triangles = edges.find(
  """(a) - [] -> (b);
    |(b) - [] -> (c);
    |(a) - [] -> (c)""".stripMargin)
triangles.limit(10).show()
// Shows a dataset with 6 columns: `a`, `b`, `c`, `ab_id`, `bc_id` and `ac_id`.
