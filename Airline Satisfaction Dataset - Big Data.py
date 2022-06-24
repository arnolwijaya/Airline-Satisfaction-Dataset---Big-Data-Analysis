#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
findspark.find()


# In[2]:


import pandas as pd
import numpy as np
import matplotlib
import seaborn as sns
import matplotlib.pyplot as plt


# In[3]:


import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = pyspark.SparkConf().setAppName('myApp').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)


# In[4]:


sql = pyspark.SQLContext(sc)


# In[5]:


location = location = "C:/Users/Arnol/Documents/Jupyter Python Scripts/Proyek Big Data/test.csv"
df = spark.read.format("com.databricks.spark.csv").options(header=True, delimiter=",", inferSchema =True).load(location)
df.toPandas().head()


# In[6]:


df = df.drop("_c0","id")
dfp = df.toPandas()
dfp.info()


# In[7]:


dfp.describe().T


# In[8]:


##################################### Satisfaction #################################

fig, chart = plt.subplots()

chart.pie(dfp["satisfaction"].value_counts(),colors=["lightblue", "orange"],
        labels= ["Neutral or Dissatisfied", "Satisfied"], autopct='%1.1f%%')
chart.set(title="Satisfaction Chart");

dfp["satisfaction"].value_counts()


# In[9]:


##################################### Gender #################################

df.select(['Gender', 'satisfaction']).groupby('Gender').count().show()
temp = pd.crosstab(dfp['Gender'], dfp['satisfaction'],  margins=True, margins_name="Total")
temp


# In[10]:


print("Probability of Female Satisfied               : " + str(5735 / 13172 * 100) + "%")
print("Probability of Female Neutral or Dissatisfied : " + str(7437 / 13172 * 100) + "%")
print("Probability of Male Satisfied                 : " + str(5668 / 12804 * 100) + "%")
print("Probability of Male Neutral or Dissatisfied   : " + str(7136 / 12804 * 100) + "%")


# In[11]:


#ct = pd.crosstab(dfp['satisfaction'],dfp['Gender'])
#ct.plot.bar(rot=0)
#for c in ct.containers:
    
    # set the bar label
   # ct.bar_label(c, label_type='center')


# In[12]:


ct = pd.crosstab(dfp['satisfaction'],dfp['Gender'])
barplot = ct.plot.bar(rot=0)

##Pakai Crossbar##
barplot = temp.plot.bar(rot=0)


# In[13]:


ct.plot.pie(figsize=(10, 10),subplots=True, autopct='%.2f%%', labeldistance=None)

plt.show()


# In[14]:


##################################### Customer Type #################################

df.select(['Customer Type', 'satisfaction']).groupby('Customer Type').count().show()
temp = pd.crosstab(dfp['Customer Type'], dfp['satisfaction'],  margins=True, margins_name="Total")
temp


# In[15]:


print("Probability of Loyal Customer Satisfied                     : " + str("%.2f" % (10195 / 21177 * 100)) + "%")
print("Probability of Loyal Customer Neutral or Dissatisfied       : " + str("%.2f" % (10982 / 21177 * 100)) + "%")
print("Probability of disloyal Customer Satisfied                  : " + str("%.2f" % (1208 / 4799 * 100)) + "%")
print("Probability of disloyal Customer Neutral or Dissatisfied    : " + str("%.2f" % (3591 / 4799 * 100)) + "%")


# In[16]:


ct = pd.crosstab(dfp['satisfaction'],dfp['Customer Type'])
barplot = ct.plot.bar(rot=0)

##Pakai Crossbar##
barplot = temp.plot.bar(rot=0)

ct.plot.pie(figsize=(10, 10),subplots=True, autopct='%.2f%%', labeldistance=None)

plt.show()


# In[17]:


################################# Type of Travel #################################

df.select(['Type of Travel', 'satisfaction']).groupby('Type of Travel').count().show()
temp = pd.crosstab(dfp['Type of Travel'], dfp['satisfaction'],  margins=True, margins_name="Total")
temp


# In[18]:


print("Probability of Business travel Satisfied                  : " + str("%.2f" % (10195 / 18038 * 100)) + "%")
print("Probability of Business travel Neutral or Dissatisfied    : " + str("%.2f" % (7428 / 18038 * 100)) + "%")
print("Probability of Personal Travel Satisfied                  : " + str("%.2f" % (793 / 7938 * 100)) + "%")
print("Probability of Personal Travel Neutral or Dissatisfied    : " + str("%.2f" % (7145 / 7938 * 100)) + "%")


# In[19]:


ct = pd.crosstab(dfp['satisfaction'],dfp['Type of Travel'])
barplot = ct.plot.bar(rot=0)

##Pakai Crossbar##
barplot = temp.plot.bar(rot=0)

ct.plot.pie(figsize=(10, 10),subplots=True, autopct='%.2f%%', labeldistance=None)

plt.show()


# In[20]:


############################### Class #################################

df.select(['Class', 'satisfaction']).groupby('Class').count().show()
temp = pd.crosstab(dfp['Class'], dfp['satisfaction'],  margins=True, margins_name="Total")
temp


# In[21]:


print("Probability of Business Satisfied                  : " + str("%.2f" % (8686 / 12495 * 100)) + "%")
print("Probability of Business Neutral or Dissatisfied    : " + str("%.2f" % (3809 / 12495 * 100)) + "%")
print("Probability of Eco  Satisfied                      : " + str("%.2f" % (2242 / 11564 * 100)) + "%")
print("Probability of Eco  Neutral or Dissatisfied        : " + str("%.2f" % (9322 / 11564 * 100)) + "%")
print("Probability of Eco Plus  Satisfied                 : " + str("%.2f" % (475 / 1917 * 100)) + "%")
print("Probability of Eco Plus  Neutral or Dissatisfied   : " + str("%.2f" % (1442 / 1917 * 100)) + "%")


# In[22]:


ct = pd.crosstab(dfp['satisfaction'],dfp['Class'])
barplot = ct.plot.bar(rot=0)

##Pakai Crossbar##
barplot = temp.plot.bar(rot=0)

ct.plot.pie(figsize=(10, 10),subplots=True, autopct='%1.1f%%', labeldistance=None)

plt.show()


# In[23]:


###################################### Age ALL ####################################
age_histogram_satisfied = df.select('Age').rdd.flatMap(lambda x: x).histogram(10)

pd.DataFrame(
    list(zip(*age_histogram_satisfied)), 
    columns=['bin', 'frequency']
).set_index(
    'bin'
).plot(kind='bar');


# In[24]:


###################################### Age Satisfied ####################################

age_histogram_satisfied = df.select('Age').where(df['satisfaction'] == 'satisfied').rdd.flatMap(lambda x: x).histogram(10)

pd.DataFrame(
    list(zip(*age_histogram_satisfied)), 
    columns=['bin', 'frequency']
).set_index(
    'bin'
).plot(kind='bar');


# In[25]:


###################################### Age neutral or dissatisfied ####################################

age_histogram_satisfied = df.select('Age').where(df['satisfaction'] == 'neutral or dissatisfied').rdd.flatMap(lambda x: x).histogram(10)

pd.DataFrame(
    list(zip(*age_histogram_satisfied)), 
    columns=['bin', 'frequency']
).set_index(
    'bin'
).plot(kind='bar');


# In[26]:


################################## Flight Distance >= 1744 ##################################

df.select(['Gender', 'Age', 'Class', 'Flight Distance', 'satisfaction']).where(df['Flight Distance'] >= 1744).show() 


# In[27]:


############################# Total Delay = Departure Delay + Arrival Delay #############################

df = df.withColumn('Total Delay', df['Departure Delay in Minutes'] + df['Arrival Delay in Minutes'])
df.select(['Departure Delay in Minutes', 'Arrival Delay in Minutes', 'Total Delay', 'Flight Distance', 'satisfaction']).where(df['Flight Distance'] == 160).show()


# In[28]:


################################# Features #################################

df = df.withColumn('satisfied', df['satisfaction'] == 'satisfied')
df = df.withColumn('neutral or dissatisfied', df['satisfaction'] == 'neutral or dissatisfied')

from pyspark.sql.types import IntegerType

df = df.withColumn('satisfied', df['satisfied'].cast(IntegerType()))
df = df.withColumn('neutral or dissatisfied', df['neutral or dissatisfied'].cast(IntegerType()))


# In[29]:


################################# Inflight wifi service #############################

## Hipotesa Not Satisfied <=3 ##

df.select(['Inflight wifi service', 'satisfied', 'neutral or dissatisfied']).groupby('Inflight wifi service').sum().sort(['Inflight wifi service']).where(df['Inflight wifi service'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Inflight wifi service', 'satisfied', 'neutral or dissatisfied']).groupby('Inflight wifi service').sum().sort(['Inflight wifi service']).where(df['Inflight wifi service'] >= 4).show()

print("Probability of Inflight wifi service Satisfied (0-3)               : " + str("%.2f" % (5523/ 18108 * 100) + "%"))
print("Probability of Inflight wifi service Neutral or Dissatisfied (0-3) : " + str("%.2f" % (12585 / 18108 * 100) + "%"))

print("Probability of Inflight wifi service Satisfied (4-5)               : " + str("%.2f" % (5880 / 7868 * 100) + "%"))
print("Probability of Inflight wifi service Neutral or Dissatisfied (4-5) : " + str("%.2f" % (1988 / 7868 * 100) + "%"))

ct = pd.crosstab(dfp['satisfaction'],dfp['Inflight wifi service'])
barplot = ct.plot.bar(rot=0)


# In[30]:


############################### Departure/Arrival time convenient #########################################

## Hipotesa Not Satisfied <=3 ##

df.select(['Departure/Arrival time convenient', 'satisfied', 'neutral or dissatisfied']).groupby('Departure/Arrival time convenient').sum().sort(['Departure/Arrival time convenient']).where(df['Departure/Arrival time convenient'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Departure/Arrival time convenient', 'satisfied', 'neutral or dissatisfied']).groupby('Departure/Arrival time convenient').sum().sort(['Departure/Arrival time convenient']).where(df['Departure/Arrival time convenient'] >= 4).show()

print("Probability of Departure/Arrival time convenient Satisfied (0-3)               : " + str("%.2f" % (6557/ 14047 * 100)) + "%")
print("Probability of Departure/Arrival time convenient Neutral or Dissatisfied (0-3) : " + str("%.2f" % (7490 / 14047 * 100)) + "%")

print("Probability of Departure/Arrival time convenient Satisfied (4-5)               : " + str("%.2f" % (4846 / 11929 * 100)) + "%")
print("Probability of Departure/Arrival time convenient Neutral or Dissatisfied (4-5) : " + str("%.2f" % (7083 / 11929 * 100)) + "%")

ct = pd.crosstab(dfp['satisfaction'],dfp['Departure/Arrival time convenient'])
barplot = ct.plot.bar(rot=0)


# In[31]:


############################ Ease of Online booking ######################################

## Hipotesa Not Satisfied <=3 ##

df.select(['Ease of Online booking', 'satisfied', 'neutral or dissatisfied']).groupby('Ease of Online booking').sum().sort(['Ease of Online booking']).where(df['Ease of Online booking'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Ease of Online booking', 'satisfied', 'neutral or dissatisfied']).groupby('Ease of Online booking').sum().sort(['Ease of Online booking']).where(df['Ease of Online booking'] >= 4).show()

print("Probability of Ease of Online booking Satisfied (0-3)               : " + str("%.2f" % (6224/ 17530 * 100)) + "%")
print("Probability of Ease of Online booking Neutral or Dissatisfied (0-3) : " + str("%.2f" % (11306 / 17530 * 100)) + "%")

print("Probability of Ease of Online booking Satisfied (4-5)               : " + str("%.2f" % (5179 / 8446 * 100)) + "%")
print("Probability of Ease of Online booking Neutral or Dissatisfied (4-5) : " + str("%.2f" % (3267 / 8446 * 100)) + "%")

ct = pd.crosstab(dfp['satisfaction'],dfp['Ease of Online booking'])
barplot = ct.plot.bar(rot=0)


# In[32]:


############################### Gate location ########################

## Hipotesa Not Satisfied <=3 ##

df.select(['Gate location', 'satisfied', 'neutral or dissatisfied']).groupby('Gate location').sum().sort(['Gate location']).where(df['Gate location'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Gate location', 'satisfied', 'neutral or dissatisfied']).groupby('Gate location').sum().sort(['Gate location']).where(df['Gate location'] >= 4).show()

print("Probability of Gate location Satisfied (0-3)               : " + str("%.2f" % (7045/ 16406 * 100)) + "%")
print("Probability of Gate location Neutral or Dissatisfied (0-3) : " + str("%.2f" % (9361 / 16406 * 100)) + "%")

print("Probability of Gate location Satisfied (4-5)               : " + str("%.2f" % (4358 / 9570  * 100)) + "%")
print("Probability of Gate location Neutral or Dissatisfied (4-5) : " + str("%.2f" % (5212 / 9570  * 100)) + "%")

ct = pd.crosstab(dfp['satisfaction'],dfp['Gate location'])
barplot = ct.plot.bar(rot=0)


# In[33]:


######################### Food and drink ###########################

## Hipotesa Not Satisfied <=3 ##

df.select(['Food and drink', 'satisfied', 'neutral or dissatisfied']).groupby('Food and drink').sum().sort(['Food and drink']).where(df['Food and drink'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Food and drink', 'satisfied', 'neutral or dissatisfied']).groupby('Food and drink').sum().sort(['Food and drink']).where(df['Food and drink'] >= 4).show()

print("Probability of Food and drink Satisfied (0-3)               : " + str("%.2f" % (4969/ 14128 * 100) + "%"))
print("Probability of Food and drink Neutral or Dissatisfied (0-3) : " + str("%.2f" % (9159 / 14128 * 100) + "%"))

print("Probability of Food and drink Satisfied (4-5)               : " + str("%.2f" % (6434 / 11848 * 100) + "%"))
print("Probability of Food and drink Neutral or Dissatisfied (4-5) : " + str("%.2f" % (5414 / 11848 * 100) + "%"))

ct = pd.crosstab(dfp['satisfaction'],dfp['Food and drink'])
barplot = ct.plot.bar(rot=0)


# In[34]:


################################# Online boarding #############################

## Hipotesa Not Satisfied <=3 ##

df.select(['Online boarding', 'satisfied', 'neutral or dissatisfied']).groupby('Online boarding').sum().sort(['Online boarding']).where(df['Online boarding'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Online boarding', 'satisfied', 'neutral or dissatisfied']).groupby('Online boarding').sum().sort(['Online boarding']).where(df['Online boarding'] >= 4).show()

print("Probability of Online boarding Satisfied (0-3)               : " + str("%.2f" % (2008/ 12963 * 100) + "%"))
print("Probability of Online boarding Neutral or Dissatisfied (0-3) : " + str("%.2f" % (10955 / 12963 * 100) + "%"))

print("Probability of Online boarding Satisfied (4-5)               : " + str("%.2f" % (9395 / 13013 * 100) + "%"))
print("Probability of Online boarding Neutral or Dissatisfied (4-5) : " + str("%.2f" % (3618 / 13013 * 100) + "%"))

ct = pd.crosstab(dfp['satisfaction'],dfp['Online boarding'])
barplot = ct.plot.bar(rot=0)


# In[35]:


################################# Seat comfort ##########################\

## Hipotesa Not Satisfied <=3 ##

df.select(['Seat comfort', 'satisfied', 'neutral or dissatisfied']).groupby('Seat comfort').sum().sort(['Seat comfort']).where(df['Seat comfort'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Seat comfort', 'satisfied', 'neutral or dissatisfied']).groupby('Seat comfort').sum().sort(['Seat comfort']).where(df['Seat comfort'] >= 4).show()

print("Probability of Seat comfort Satisfied (0-3)               : " + str("%.2f" % (2567/ 11297 * 100) + "%"))
print("Probability of Seat comfort Neutral or Dissatisfied (0-3) : " + str("%.2f" % (8730 / 11297 * 100) + "%"))

print("Probability of Seat comfort Satisfied (4-5)               : " + str("%.2f" % (8836 / 14679 * 100) + "%"))
print("Probability of Seat comfort Neutral or Dissatisfied (4-5) : " + str("%.2f" % (5843 / 14679 * 100) + "%"))

ct = pd.crosstab(dfp['satisfaction'],dfp['Seat comfort'])
barplot = ct.plot.bar(rot=0)


# In[36]:


############################### Inflight entertainment ############################

## Hipotesa Not Satisfied <=3 ##

df.select(['Inflight entertainment', 'satisfied', 'neutral or dissatisfied']).groupby('Inflight entertainment').sum().sort(['Inflight entertainment']).where(df['Inflight entertainment'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Inflight entertainment', 'satisfied', 'neutral or dissatisfied']).groupby('Inflight entertainment').sum().sort(['Inflight entertainment']).where(df['Inflight entertainment'] >= 4).show()

print("Probability of Inflight entertainment Satisfied (0-3)               : " + str("%.2f" % (2745/ 12277 * 100) + "%"))
print("Probability of Inflight entertainment Neutral or Dissatisfied (0-3) : " + str("%.2f" % (9532 / 12277 * 100) + "%"))

print("Probability of Inflight entertainment Satisfied (4-5)               : " + str("%.2f" % (8658 / 13699 * 100) + "%"))
print("Probability of Inflight entertainment Neutral or Dissatisfied (4-5) : " + str("%.2f" % (5041 / 13699 * 100) + "%"))

ct = pd.crosstab(dfp['satisfaction'],dfp['Inflight entertainment'])
barplot = ct.plot.bar(rot=0)


# In[37]:


############################# On-board service ###########################

## Hipotesa Not Satisfied <=3 ##

df.select(['On-board service', 'satisfied', 'neutral or dissatisfied']).groupby('On-board service').sum().sort(['On-board service']).where(df['On-board service'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['On-board service', 'satisfied', 'neutral or dissatisfied']).groupby('On-board service').sum().sort(['On-board service']).where(df['On-board service'] >= 4).show()

print("Probability of On-board service Satisfied (0-3)               : " + str("%.2f" % (3413/ 12296 * 100)) + "%")
print("Probability of On-board service Neutral or Dissatisfied (0-3) : " + str("%.2f" % (8883 / 12296 * 100)) + "%")

print("Probability of On-board service Satisfied (4-5)               : " + str("%.2f" % (7990 / 13680 * 100)) + "%")
print("Probability of On-board service Neutral or Dissatisfied (4-5) : " + str("%.2f" % (5690 / 13680 * 100)) + "%")

ct = pd.crosstab(dfp['satisfaction'],dfp['On-board service'])
barplot = ct.plot.bar(rot=0)


# In[38]:


######################### Leg room service ##########################

## Hipotesa Not Satisfied <=3 ##

df.select(['Leg room service', 'satisfied', 'neutral or dissatisfied']).groupby('Leg room service').sum().sort(['Leg room service']).where(df['Leg room service'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Leg room service', 'satisfied', 'neutral or dissatisfied']).groupby('Leg room service').sum().sort(['Leg room service']).where(df['Leg room service'] >= 4).show()

print("Probability of Leg room service Satisfied (0-3)               : " + str("%.2f" % (3406/ 12641 * 100)) + "%")
print("Probability of Leg room service Neutral or Dissatisfied (0-3) : " + str("%.2f" % (9235 / 12641 * 100)) + "%")

print("Probability of Leg room service Satisfied (4-5)               : " + str("%.2f" % (7997 / 13335 * 100)) + "%")
print("Probability of Leg room service Neutral or Dissatisfied (4-5) : " + str("%.2f" % (5338 / 13335 * 100)) + "%")

ct = pd.crosstab(dfp['satisfaction'],dfp['Leg room service'])
barplot = ct.plot.bar(rot=0)


# In[39]:


################################## Baggage handling ############################

## Hipotesa Not Satisfied <=3 ##

df.select(['Baggage handling', 'satisfied', 'neutral or dissatisfied']).groupby('Baggage handling').sum().sort(['Baggage handling']).where(df['Baggage handling'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Baggage handling', 'satisfied', 'neutral or dissatisfied']).groupby('Baggage handling').sum().sort(['Baggage handling']).where(df['Baggage handling'] >= 4).show()

print("Probability of Baggage handling Satisfied (0-3)               : " + str("%.2f" % (2663/ 9851 * 100)) + "%")
print("Probability of Baggage handling Neutral or Dissatisfied (0-3) : " + str("%.2f" % (7188 / 9851 * 100)) + "%")

print("Probability of Baggage handling Satisfied (4-5)               : " + str("%.2f" % (8740/ 16125 * 100)) + "%")
print("Probability of Baggage handling Neutral or Dissatisfied (4-5) : " + str("%.2f" % (7385 / 16125 * 100)) + "%")

ct = pd.crosstab(dfp['satisfaction'],dfp['Baggage handling'])
barplot = ct.plot.bar(rot=0)


# In[40]:


################################ Checkin service ##########################

## Hipotesa Not Satisfied <=3 ##

df.select(['Checkin service', 'satisfied', 'neutral or dissatisfied']).groupby('Checkin service').sum().sort(['Checkin service']).where(df['Checkin service'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Checkin service', 'satisfied', 'neutral or dissatisfied']).groupby('Checkin service').sum().sort(['Checkin service']).where(df['Checkin service'] >= 4).show()

print("Probability of Checkin service Satisfied (0-3)               : " + str("%.2f" % (4792/ 13434 * 100)) + "%")
print("Probability of Checkin service Neutral or Dissatisfied (0-3) : " + str("%.2f" % (8642 / 13434 * 100)) + "%")

print("Probability of Checkin service Satisfied (4-5)               : " + str("%.2f" % (6611/ 12542 * 100)) + "%")
print("Probability of Checkin service Neutral or Dissatisfied (4-5) : " + str("%.2f" % (5931 / 12542 * 100)) + "%")

ct = pd.crosstab(dfp['satisfaction'],dfp['Checkin service'])
barplot = ct.plot.bar(rot=0)


# In[41]:


####################### Inflight service #############################

## Hipotesa Not Satisfied <=3 ##

df.select(['Inflight service', 'satisfied', 'neutral or dissatisfied']).groupby('Inflight service').sum().sort(['Inflight service']).where(df['Inflight service'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Inflight service', 'satisfied', 'neutral or dissatisfied']).groupby('Inflight service').sum().sort(['Inflight service']).where(df['Inflight service'] >= 4).show()

print("Probability of Inflight service Satisfied (0-3)               : " + str("%.2f" % (2620/ 9648 * 100)) + "%")
print("Probability of Inflight service Neutral or Dissatisfied (0-3) : " + str("%.2f" % (7028 / 9648 * 100)) + "%")

print("Probability of Inflight service Satisfied (4-5)               : " + str("%.2f" % (8788/ 16328 * 100)) + "%")
print("Probability of Inflight service Neutral or Dissatisfied (4-5) : " + str("%.2f" % (7545 / 16328 * 100))+ "%")

ct = pd.crosstab(dfp['satisfaction'],dfp['Inflight service'])
barplot = ct.plot.bar(rot=0)


# In[42]:


######################### Cleanliness #######################

## Hipotesa Not Satisfied <=3 ##

df.select(['Cleanliness', 'satisfied', 'neutral or dissatisfied']).groupby('Cleanliness').sum().sort(['Cleanliness']).where(df['Cleanliness'] <= 3).show()

## Hipotesa Satisfied >3 ##

df.select(['Cleanliness', 'satisfied', 'neutral or dissatisfied']).groupby('Cleanliness').sum().sort(['Cleanliness']).where(df['Cleanliness'] >= 4).show()

print("Probability of Cleanliness Satisfied (0-3)               : " + str("%.2f" % (4179/ 13459 * 100)) + "%")
print("Probability of Cleanliness Neutral or Dissatisfied (0-3) : " + str("%.2f" % (9280 / 13459 * 100)) + "%")

print("Probability of Cleanliness Satisfied (4-5)               : " + str("%.2f" % (7224/ 12517 * 100)) + "%")
print("Probability of Cleanliness Neutral or Dissatisfied (4-5) : " + str("%.2f" % (5293 / 12517 * 100)) + "%")

ct = pd.crosstab(dfp['satisfaction'],dfp['Cleanliness'])
barplot = ct.plot.bar(rot=0)


# In[43]:


#df.show()
#indexing categorical values
from pyspark.sql.functions import col
from pyspark.sql.functions import when
df= df.withColumn("Gender", when(col("Gender")=='Male', 1).otherwise(0))
df = df.withColumn("Customer Type", when(col("Customer Type")=='Loyal Customer', 1).otherwise(0))
df = df.withColumn("Type of Travel", when(col("Type of Travel")=='Business travel', 1).otherwise(0))
df = df.withColumn("Class", when(col("Class")=='Business', 2).otherwise( when(col("Class")=='Eco Plus', 1).otherwise(0)))
df = df.withColumn("Satisfaction", when(col("Satisfaction")=='satisfied', 1).otherwise(0))
#df.show()
dff = df.select("*").toPandas()
dff
dff.astype(float)
corr = dff.astype('float64').corr()
corr
plt.figure(figsize=(20, 10))
# define the mask to set the values in the upper triangle to True
mask = np.triu(np.ones_like(corr))
heatmap = sns.heatmap(corr, mask=mask, vmin=-1, vmax=1, annot=True, cmap='BrBG')
heatmap.set_title('Triangle Correlation Heatmap', fontdict={'fontsize':18}, pad=16);


# In[44]:


#drop Gender, Departure/Arrival time convenient, Gate location, Departure Delay in Minutes, Arrival Delay in Minutes, Total Delay
#Nilai korelasi terhadap satisfaction mendekati 0

df_dt = df.select(['Customer Type', 'Age','Type of Travel', 'Class', 'Flight Distance','Inflight wifi service', 'Ease of Online booking', 'Food and drink', 'Online boarding', 'Seat comfort', 'Inflight entertainment', 'On-board service', 'Leg room service', 'Baggage handling', 'Checkin service', 'Inflight service', 'Cleanliness', 'Satisfaction'])


# In[45]:


from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler


# In[46]:


columns_to_scale = ['Customer Type', 'Age','Type of Travel', 'Class', 'Flight Distance','Inflight wifi service', 'Ease of Online booking', 'Food and drink', 'Online boarding', 'Seat comfort', 'Inflight entertainment', 'On-board service', 'Leg room service', 'Baggage handling', 'Checkin service', 'Inflight service', 'Cleanliness']
assemblers = [VectorAssembler(inputCols=[col], outputCol=col + "_vec") for col in columns_to_scale]
scalers = [MinMaxScaler(inputCol=col + "_vec", outputCol=col + "_scaled") for col in columns_to_scale]
pipeline = Pipeline(stages=assemblers + scalers)
scalerModel = pipeline.fit(df_dt)
scaledData = scalerModel.transform(df_dt)


# In[47]:


assemblerInputs = ['Customer Type', 'Age','Type of Travel', 'Class', 'Flight Distance','Inflight wifi service', 'Ease of Online booking', 'Food and drink', 'Online boarding', 'Seat comfort', 'Inflight entertainment', 'On-board service', 'Leg room service', 'Baggage handling', 'Checkin service', 'Inflight service', 'Cleanliness']
vector_assembler = VectorAssembler(inputCols=assemblerInputs,outputCol="Features")
assembler_temp = vector_assembler.transform(df_dt)


# In[48]:


assembler = assembler_temp.drop('Customer Type', 'Age','Type of Travel', 'Class', 'Flight Distance','Inflight wifi service', 'Ease of Online booking', 'Food and drink', 'Online boarding', 'Seat comfort', 'Inflight entertainment', 'On-board service', 'Leg room service', 'Baggage handling', 'Checkin service', 'Inflight service', 'Cleanliness')


# In[49]:


##Bagi Dataset 80,20
(trainDataset, testDataset) = assembler.randomSplit([0.8,0.2])


# In[50]:


from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


# In[67]:


##Algo Decision Tree Cllassifier
dt_model = DecisionTreeClassifier(labelCol="Satisfaction", featuresCol="Features")
model_dt = dt_model.fit(trainDataset)

dt_predictions = model_dt.transform(testDataset)
dt_predictions.show()
#predictions.where(predictions['Satisfaction']==1).show()


# In[68]:


evaluator = MulticlassClassificationEvaluator(labelCol="Satisfaction", predictionCol="prediction")
accuracy = evaluator.evaluate(dt_predictions)
print("Accuracy : ", accuracy)


# In[69]:


model_dt.featureImportances


# In[70]:


############### GBT ########################

from pyspark.ml.classification import GBTClassifier

gbt_model = GBTClassifier(labelCol="Satisfaction", featuresCol="Features", maxIter=10)
model_gbt = gbt_model.fit(trainDataset)

gbt_predictions = model_gbt.transform(testDataset)
gbt_predictions.show()


# In[76]:


evaluator = MulticlassClassificationEvaluator(labelCol="Satisfaction", predictionCol="prediction")
accuracy = evaluator.evaluate(gbt_predictions)
print("Accuracy : ", accuracy)


# In[72]:


model_gbt.featureImportances


# In[73]:


from pyspark.ml.classification import LinearSVC

svm_model = GBTClassifier(labelCol="Satisfaction", featuresCol="Features", maxIter=10)
model_svm = svm_model.fit(trainDataset)

svm_predictions = model_svm.transform(testDataset)
svm_predictions.show()


# In[77]:


evaluator = MulticlassClassificationEvaluator(labelCol="Satisfaction", predictionCol="prediction")
accuracy = evaluator.evaluate(svm_predictions)
print("Accuracy : ", accuracy)


# In[75]:


model_svm.featureImportances

