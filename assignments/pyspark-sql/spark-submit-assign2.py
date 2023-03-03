from pyspark import SparkSession

spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()

df = spark.read.csv("/home/tech/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv",header=True,sep=';',escape=',',inferSchema=True)

df.show()

df.createOrReplaceTempView('bankmarket')

df1=spark.sql('select *from bankmarket')

df1.show()

df2=spark.sql("select (select count(y) from bankmarket where y='yes')/count(y) success_rate from bankmarket")

df2.show()

df3=spark.sql("select (select count(y) from bankmarket where y='no')/count(y) fail_rate from bankmarket")
 
df3.show()

df4=spark.sql("select max(age) old,avg(age) middleaged,min(age) young from bankmarket")

df4.show()

df6=spark.sql("select age,count(y) sub_count from bankmarket where y='yes' group by age")

df6.show()

df7=spark.sql("select age,count(y) count,case when age<19 then 'kids' when age>19 and age<29 then 'youth' when age>29 and age<40 then 'middleage' else 'old' end as agegroup from bankmarket where y='yes' group by age,agegroup")

df7.show()

df8=spark.sql("select age,marital,count(y) sub_count from bankmarket where y='yes' group by age,marital")

df8.show()
 
df9=spark.sql("select marital,count(y) sub_count from bankmarket where y='yes' group by marital")
 
df9.show()