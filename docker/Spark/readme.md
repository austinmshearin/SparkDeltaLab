# To utilize this local Spark (version 3.5.1) cluster running in Docker:
- Must install Java 17 (https://www.oracle.com/java/technologies/downloads/archive/)
    - Java is required on the local machine as PySpark utilizes Java to open the connection to the Spark cluster
    - Java 17 is required as Spark is only compatible with Java 8, 11, and 17 (https://community.cloudera.com/t5/Community-Articles/Spark-and-Java-versions-Supportability-Matrix/ta-p/383669)
    - Verify you are able to run `java -version` in your terminal which should return version 17
- Must use Python 3.11.9 locally
    - The bitnami/spark:3.5.1 image uses this version of Python and the versions must match between the cluster and the client
    - At a minimum, the minor version must match or the connection will error out
- Must use PySpark==3.5.1
    - This version of pyspark is set within the requirements.txt file and must be used to match the Spark version

# To create a PySpark client, you must set the context explicitly
```
import pyspark
from pyspark.conf import SparkConf

spark_conf = SparkConf()
spark_conf.setAll([
    ("spark.master", "spark://localhost:7077"), # The address of the master node which is set within the docker compose file
    ("spark.submit.deployMode", "client"), # Client mode indicates the local host is the driver program (should be client by default)
    ("spark.driver.bindAddress", "0.0.0.0"), # Binds the driver to all available network interfaces
    ("spark.app.name", "spark-local-cluster"), # The name of the application that will display in the Spark UI
    ("spark.executor.memory", "4g") # Explicitly sets the memory allocated to the executor in the cluster (can't exceed amount allocated in the docker compose file)
])

spark = pyspark.sql.SparkSession.builder.config(conf=spark_conf).getOrCreate()
```