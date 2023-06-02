from pyspark.sql import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
sc = spark.sparkContext

#dbtable="COUNTRY"

#URL=jdbc:postgresql://db1.c6b1uj4uhdxo.ap-south-1.rds.amazonaws.com:5432/pdb

host="jdbc:postgresql://db1.c6b1uj4uhdxo.ap-south-1.rds.amazonaws.com:5432/pdb"
user="puser"
pwd="ppassword"
driver="org.postgresql.Driver"

print("Data import for reference table is started")
#Country Table
df_cn=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","CARD").load()
df_cn.write.format("org.apache.phoenix.spark").option("table","CARD_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("Country Table Imported Successfully")

#City Table
df_ct=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","CC_Debit").load()
df_ct.write.format("org.apache.phoenix.spark").option("table","CC_DEBIT_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("City Table Imported Successfully")

#Card_Type Table
df_cardtype=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","CC_Paid").load()
df_cardtype.write.format("org.apache.phoenix.spark").option("table","CC_PAID_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("Card_Type Table Imported Successfully")

#Tx_Type Table
df_txtype=spark.read.format("jdbc").option("url",host).option("user",user).option("password",pwd).option("driver",driver).option("dbtable","Address").load()
df_txtype.write.format("org.apache.phoenix.spark").option("table","ADDRESS_HB").option("zkUrl","localhost:2181").mode('overwrite').save()
print("Tx_Type Table Imported Successfully")