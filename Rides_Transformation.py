#!/usr/bin/env python
# coding: utf-8

# ## Notebook 1
# 
# New notebook

# In[7]:


from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()


# In[45]:


data_rides = spark.read.csv("abfss://0ea7f5cf-a6a8-40ee-9f5a-0d4d5f8df89b@onelake.dfs.fabric.microsoft.com/145709cd-3a50-4f3f-b229-b9a5e3ea924d/Files/UberRawData/Rides.csv", header = True, inferSchema=True)


# In[38]:


from pyspark.sql.functions import *


# In[48]:


data_rides = data_rides.withColumn("start_datetime", to_timestamp(data_rides["ride_start_time"], "dd-MM-yyyy HH:mm"))


# In[86]:


display(data_rides.limit(2))


# In[73]:


data_rides = data_rides.withColumn("end_datetime", to_timestamp(data_rides["ride_end_time"], "dd-MM-yyyy HH:mm"))


# In[59]:


data_rides = data_rides.withColumn("startt_Date", date_format(data_rides['ride_start_ime'], "yyyy-MM-dd"))


# In[77]:


data_rides = data_rides.withColumn("endd_Date", date_format(data_rides['end_datetime'], "yyyy-MM-dd"))


# In[78]:


data_rides = data_rides.withColumn("endd_time", date_format(data_rides['end_datetime'], "HH:mm"))


# In[65]:


data_rides = data_rides.withColumn("startt_time", date_format(data_rides['start_datetime'], "HH:mm"))


# %md
# ### Calculate duration in minutes

# In[75]:


data_rides = data_rides.withColumn("duration_in_minutes", (unix_timestamp(data_rides['end_datetime']) - unix_timestamp(data_rides['start_datetime']))/60)


# %md
# ### Calculate duration in hours

# In[82]:


data_rides = data_rides.withColumn("duration_in_hours", (unix_timestamp(data_rides['end_datetime']) - unix_timestamp(data_rides['start_datetime']))/(60*60))


# In[103]:


display(data_rides.limit(2))


# 

# In[85]:


data_rides = data_rides.withColumn("Revenue", data_rides['Fare']*0.25)


# In[87]:


data_rides = data_rides.withColumn("pickup_hour", hour(data_rides["start_datetime"]))


# In[91]:


display(data_rides.limit(2))


# %md
# ### calculate is_Night ride or not , >23 and <5

# In[90]:


data_rides = data_rides.withColumn(
    "is_Night_ride",
    when((data_rides["pickup_hour"] > 23) | (data_rides["pickup_hour"] < 5), "Yes").otherwise("No")
)


# %md
# ### find day of week ( from 1 (sun) to 7(sat))

# In[93]:


data_rides = data_rides.withColumn(
    "day_of_week", dayofweek(data_rides['start_datetime']))


# %md
# ### find is weekend yes or No (1 = Sun, 7 = Sat)

# In[101]:


data_rides = data_rides.withColumn("is_weekend",
    when((data_rides["day_of_week"]==1) & (data_rides["day_of_week"]==7), "Yes").otherwise("No")
)


# In[102]:


data_rides = data_rides.withColumn("Ride_Time_category",
      when((data_rides["distance (km)"]>0) & (data_rides["distance (km)"]<5),"Short")
      .when((data_rides["distance (km)"]>5) & (data_rides["distance (km)"]<10),"Medium")
      .when((data_rides["distance (km)"]>10) & (data_rides["distance (km)"]<20), "Long")
      .otherwise("Far"))


# In[106]:


data_rides.coalesce(1)
.write.format("csv")
.mode("overwrite")
.save("abfss://0ea7f5cf-a6a8-40ee-9f5a-0d4d5f8df89b@onelake.dfs.fabric.microsoft.com/145709cd-3a50-4f3f-b229-b9a5e3ea924d/Files/Databricks_files")

