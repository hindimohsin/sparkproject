from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local").appName("test").getOrCreate()
sc = spark.sparkContext

data = "C://bigdata//datasets//donations.csv"
output = ""

df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
df.show(5, False)
df.printSchema()

res = df.withColumn("cdate", current_date()).drop("date")
res.show(5)
res.printSchema()

df.createOrReplaceTempView("tab")
res = spark.sql("sle")


