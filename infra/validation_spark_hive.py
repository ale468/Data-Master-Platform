from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("validation-spark-hive") \
    .master("k8s://https://kubernetes.default.svc") \
    .config("spark.kubernetes.namespace", "data-platform") \
    .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.data-platform.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore.data-platform.svc.cluster.local:9083") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.5,com.amazonaws:aws-java-sdk-bundle:1.12.512") \
    .getOrCreate()

# write
df = spark.createDataFrame([(1, 'a'), (2, 'b'), (3, 'c')], ['id', 'value'])
df.write.mode('overwrite').parquet('s3a://bronze/test/data.parquet')

# read back from s3
back = spark.read.parquet('s3a://bronze/test/data.parquet')
back.show()

# register hive table
df.write.mode('overwrite').saveAsTable('default.test_table')

# read table via hive
df2 = spark.sql('SELECT * FROM default.test_table')
df2.show()

spark.stop()
