import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import Row, Window
import pyspark.sql.types as T
import pyspark.sql.functions as F

spark_conf = SparkConf()
spark_conf.setAll([
    ("spark.master", "spark://localhost:7077"), # The address of the master node which is set within the docker compose file
    ("spark.submit.deployMode", "client"), # Client mode indicates the local host is the driver program (should be client by default)
    ("spark.driver.bindAddress", "0.0.0.0"), # Binds the driver to all available network interfaces
    ("spark.app.name", "spark-local-cluster"), # The name of the application that will display in the Spark UI
    ("spark.executor.memory", "4g") # Explicitly sets the memory allocated to the executor in the cluster (can't exceed amount allocated in the docker compose file)
])

spark = pyspark.sql.SparkSession.builder.config(conf=spark_conf).getOrCreate()

schema = T.StructType([
    T.StructField("string_field", T.StringType(), True),
    T.StructField("integer_field", T.IntegerType(), True),
    T.StructField("float_field", T.DoubleType(), True),
    T.StructField("boolean_field", T.BooleanType(), True),
    T.StructField("array_field", T.ArrayType(T.StringType()), True),
    T.StructField("struct_field", T.StructType([
        T.StructField("sub_field", T.StringType(), True)
    ]))
])
df = spark.createDataFrame(
    [
        ["a", 1, 1.1, True, ["b"], {"sub_field": "c"}],
        ["d", 2, 2.1, False, ["e", "f"], {"sub_field": "g"}],
        ["d", 3, 3.1, True, ["h", "i", "j"], {"sub_field": "k"}]
    ],
    schema
)
df.show()