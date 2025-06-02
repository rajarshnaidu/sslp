# Print all Spark conf values from JVM
conf = sc._jvm.org.apache.spark.SparkEnv.get().conf()
conf_map = {entry._1(): entry._2() for entry in conf.getAll()}
for k, v in conf_map.items():
    print(k, "=>", v)
