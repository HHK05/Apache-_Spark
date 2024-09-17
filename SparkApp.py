#!/usr/bin/env python
# coding: utf-8

# In[2]:


#import the nessasry packages
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
import pyspark.sql.functions as func


# In[3]:


#create the session of spark that is for building te spark session one by one
Spark=SparkSession.builder.appName("FirstApp").getOrCreate()


# In[4]:


#define your schema with all teh nessary types and the values
my_schema=StructType([\
                     StructField("userID",IntegerType(),True),
                     StructField("name",StringType(),True),
                     StructField("age",StringType(),True),
                     StructField("friends",IntegerType(),True)
                     ])


# In[5]:


#creating dataframe on csv
people=Spark.read.format("csv")\
        .schema(my_schema)\
        .option("path","fakefriends.csv")\
        .load()


# In[10]:


#performing all the transformation
output=people.select(people.userID,people.name,people.age,people.friends)\
    .where(people.age<30).withColumn('insert_ts',func.current_timestamp())\
    .orderBy(people.userID)


# In[12]:


#count the number of outputs
output.count()


# In[13]:


#creating the temp view
output.createOrReplaceTempView("people")


# In[16]:


#perform the sql querry on the table
Spark.sql("select name,age,friends,insert_ts from people").show()


# In[1]:


from pyspark.sql.types import LongType

def cube(s):
    return s*s*s

spark.udf.register("cube",cube,LongType())
spark.range(1,9).createOrReplaceTempView("udf_test")
spark.sql("select id, cube(id) AS id_cube from udf_test").show()


# In[ ]:




