
import pyspark

from pyspark.sql import SparkSession


spark = SparkSession.builder\
    .appName("question1")\
    .master("local[2]")\
    .getOrCreate()

# In[3]:


df=spark.read.csv("/home/sunbeam/Downloads/nonConfidential.csv",inferSchema=True,header=True)  #schema -colounm type header -col name


# In[4]:


df1=spark.read.parquet("/home/sunbeam/Downloads/confidential.snappy.parquet",inferSchema=True,header=True)


# In[7]:


df3=df.unionAll(df1)


# In[18]:


df3.printSchema()


# In[8]:


print(df3.filter(df3.State=='VA').count())