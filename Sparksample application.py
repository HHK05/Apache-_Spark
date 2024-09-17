#!/usr/bin/env python
# coding: utf-8

# In[16]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc


# In[2]:


spark=SparkSession.builder.appName("SampleApp").getOrCreate()


# In[5]:


data=spark.read.format('csv').\
option('infraSchema',True).\
option('header',True).\
option('path','operations_management.csv').\
load()


# In[6]:


data.printSchema()


# In[17]:


data_2=data.select("industry","value").\
    filter(col("value")>10000).\
    orderBy(desc("value"))


# In[20]:


data_2.printSchema()


# In[21]:


data_2.show()


# In[28]:


data_3 = data.select("industry", "value") \
    .filter((col("value") > 200) & (col("industry") != "total")) \
    .orderBy(desc("value"))


# In[31]:


data_3.printSchema()


# In[33]:


data_3.show(25)


# In[35]:


data_2.createOrReplaceTempView("data")


# In[40]:


spark.sql("""select industry,value 
from data
where 
value>200 and industry!= "total"
""").show(5)


# In[ ]:




