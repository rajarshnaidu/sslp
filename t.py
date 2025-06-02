from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("CheckClass").setMaster("local[*]") \
    .set("spark.jars", "/full/path/to/spark-cassandra-connector-assembly_2.12-3.5.1.jar")

sc = SparkContext(conf=conf)
jvm = sc._jvm

try:
    cls = jvm.java.lang.Class.forName("com.datastax.oss.driver.api.core.CqlSession")
    print("Class found:", cls)
except Exception as e:
    print("Class not found:", e)

sc.stop()
