# Databricks notebook source
lst=[21,15,75,35,45]
type(lst)
spark.createDataFrame(lst,'int')
from pyspark.sql.types import IntegerType
spark.createDataFrame(lst,IntegerType())
sample_list=['kumar', 'Ram', 'katyhik']
from pyspark.sql.types import StringType
spark.createDataFrame(sample_list,StringType()).show()


# COMMAND ----------

lst=[21,15,75,35,45]
print(lst)

# COMMAND ----------

lst=[21,15,75,35,45]
type(lst)
spark.createDataFrame(lst,'int')
from pyspark.sql.types import IntegerType
spark.createDataFrame(lst,IntegerType()).show()

# COMMAND ----------

sample_list=['kumar', 'Ram', 'katyhik']
from pyspark.sql.types import StringType
spark.createDataFrame(sample_list,StringType()).show()

# COMMAND ----------

lst=[(21, ),(23, ),(41, ),(32, )]
type(list)
type(list[2])
spark.createDataFrame(lst,'age int')
sample_list=[(1,'kumar'),(2, 'Ram'),(3,'karthik'),(4,'sai')]
spark.createDataFrame(sample_list,'user_id int,FN String').show()

# COMMAND ----------

sample_list = [
    {'user_id': 1,'user_first_name':'kumar'},
    {'user_id': 2,'user_first_name': 'Ram'},
    {'user_id': 3,'user_first_name': 'karthik'},
    {'user_id': 4,'user_first_name': 'sai'},
    
]
spark.createDataFrame(sample_list).show()

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/rathankumarvadla@gmail.com/circuits.csv")

# COMMAND ----------

circuits= spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/rathankumarvadla@gmail.com/hospital_admissions-1.csv")

# COMMAND ----------

display(circuits)

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/rathankumarvadla@gmail.com/Kolkata_Branch-1.csv")

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/rathankumarvadla@gmail.com/circuits.csv")

# COMMAND ----------

df1.show()

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/rathankumarvadla@gmail.com/circuits.csv")

# COMMAND ----------

display(circuit2)

# COMMAND ----------

circuit24_renamed_df=circuits24.withColumnRenamed("circuit", "circit_Id")\
.withColumnRenamed("CircuitRef","Circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude")




# COMMAND ----------

cir_scheme=structType(fields=[struct])  
