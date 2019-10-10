val sparkSession = SparkSession.builder
  .master("local[1]")
  .appName("WCOJ-spark")
  .getOrCreate()

// read a dataframe
val df = sparkSession.read
  .option("inferSchema", "true")
  .csv("/path/to/edge/relationship")

// df needs columns called `edge_id`, `src` and `dst`
// Use WCOJ to find a triangle pattern
val triangles = df.findPattern(
  """(a) - [] -> (b);
    |(b) - [] -> (c);
    |(a) - [] -> (c)""".stripMargin,
  Seq("a", "b", "c")
)
triangles.limit(10).show()
// Shows a dataset with 3 columns: `a`, `b`, `c` being node ids
