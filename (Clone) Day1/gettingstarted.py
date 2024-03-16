# Databricks notebook source
print ("Hello")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema sameep

# COMMAND ----------

data=([(1,'a',30),(2,'b',34)])
schema="id int, name string, age int"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.show()

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/raw/emp.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.saveAsTable("sameep.emp_demo")

# COMMAND ----------


