from pyspark import SparkSession

spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()

from pyspark.sql.functions import *

df1=spark.read.csv(path='/home/tech/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv',sep=";",header=True,escape=",",inferSchema=True)

df1.show()

df1.createOrReplaceTempView('bankmarketdata')

df2=spark.sql('select *from bankmarketdata')

df2.show()

df4=spark.sql("select age,y,case when age<19 then 'kids' when age>19 and age<29 then 'youth' when age>29 and age<40 then 'middleage' else 'old' end as agegroup from bankmarketdata where y='yes' ")

df4.show()

df4.createOrReplaceTempView('subscription')

df5=spark.sql('select *from subscription')

df5.show()

df6=spark.sql('select agegroup,count(age) from subscription group by agegroup')

df6.show()

df6.write.parquet('/futurense_hadoop-pyspark/labs/dataset/bankmarket/parquetoutput')

df9 = spark.read.parquet('/futurense_hadoop-pyspark/labs/dataset/bankmarket/parquetoutput')

df9.show()

df7=spark.sql('select agegroup,count(age) from subscription group by agegroup having count(age)>2000')

df7.show()

df8.select('agegroup','count(age)').write.format("avro").save('/home/tech/futurense_hadoop-pyspark/labs/dataset/bankmarket/avrooutput')

df8.show()