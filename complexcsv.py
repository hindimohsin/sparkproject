from pyspark.sql import *
from pyspark.sql import functions as F
import re
spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
sc = spark.sparkContext

data = "C:\\bigdata\\datasets\\10000Records.csv"
df = spark.read.format("csv").option("inferSchema","true").option("header","true").load(data)
ndf = df.toDF(*(re.sub(r"[^a-zA-Z]","",c)for c in df.columns))
#toDF used to rename column name
ndf.printSchema()
ndf.show(4)

df.createOrReplaceTempView("tab")
res = spark.sql("select * from tab where EmpID = 198429")