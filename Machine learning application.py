#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor, LinearRegression, DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import RFormula
import pandas as pd
import click


# In[3]:


spark=SparkSession.builder.appName("machinelearningapplication").getOrCreate()


# In[6]:


filepath="C:\\Users\\Harsh\\Downloads\\ML_Data.parquet"
df=spark.read.parquet(filepath)
df.select("neighbourhood_cleansed","room_type","bedrooms","bathrooms","number_of_reviews","price").show(5)
'''or
import pandas as pd
pd.read_parquet('example_pa.parquet', engine='pyarrow')
'''


# In[10]:


x_train,y_train=df.randomSplit([.8,.2],seed=42)
print(x_train.count())
print(y_train.count())


# In[12]:


vecassembler=VectorAssembler(inputCols=["bedrooms"],outputCol="features")
vectrain=vecassembler.transform(x_train)
vectrain.select("bedrooms",'features','price').show(10)


# In[13]:


lr=LinearRegression(featuresCol="features",labelCol="price")
lrModel=lr.fit(vectrain)


# In[17]:


pipeline=Pipeline(stages=[vecassembler,lr])
pipelinemodel=pipeline.fit(x_train)


# In[19]:


pred=pipelinemodel.transform(y_train)
pred.select("bedrooms","features","price","prediction").show(10)


# In[22]:


regression_evaluator=RegressionEvaluator(
predictionCol="prediction",
labelCol='price',
metricName='rmse')
rmse=regression_evaluator.evaluate(pred)
print(rmse)


# In[23]:


r2=regression_evaluator.setMetricName("r2").evaluate(pred)
print(r2)


# In[24]:


#saving and loading the machine learning model 
pipeline_path="E:\\SPARK"
pipelinemodel.write().overwrite().save(pipeline_path)


# In[28]:


from sklearn.metrics import accuracy_score,f1_score 


# In[2]:


#acc=accuracy_score(y_train,pred)


# In[ ]:





# In[ ]:




