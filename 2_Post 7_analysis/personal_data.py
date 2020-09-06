##############################
# Working with Personal Data #
##############################

if ('sc' in locals() or 'sc' in globals()):
    print('''<<<<<!!!!! It seems that you are running in a IBM Watson Studio Apache
    Spark Notebook. Please run it in an IBM Watson Studio Default Runtime
    (without Apache Spark) !!!!! >>>>>''')

#######################
# Importing Libraries #
#######################

import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# initializing a spark object
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession \
    .builder \
    .getOrCreate()

############################################
# Importing Dataset: iPhone Healh App Data #
############################################
# reading and storing dataset
df = spark.read.csv('personal_data.csv', header="true")

# registering a corresponding query table / spark dataframe
df.createOrReplaceTempView('df')

#################################################
# Bar Chart: Counts Time Spent by Acitvity Type #
#################################################

# checking table, and schema setup
print(df.show())
print(df.printSchema())

# grouping by class to get counts
df.groupBy('Type').count().show()

# plotting bar chart of category counts of time spent by activity type
import matplotlib.pyplot as plt
from pyspark.sql.functions import col

# ordering on counts
counts = df.groupBy('Type').count().orderBy('count')

# removing this codepiece from original:
#display(counts)

# that's a pretty good jugaad!
df_pandas = counts.toPandas()

# sorting values descending
df_pandas = df_pandas.sort_values(by='count', ascending=False)

# Graph 1:  Create a horizontal bar plot
df_pandas.plot(kind='barh', x='Type', y='count', colormap='winter_r')
plt.show()

####################################################################
# Query: Spread Stats using Query string and SQL inbuilt Functions #
####################################################################

# quite a neat sophisticated basic query
# to tally up time spend on various tasks, w/ no inducation of the type itself.
spark.sql('''
    select
        *,
        max/min as minmaxratio -- computes min to max ratio
        from (
            select
                min(ct) as min, -- computes min val of all types
                max(ct) as max, -- computes max val of all types
                mean(ct) as mean, -- computes mean val b/w all types
                stddev(ct) as stddev -- computes stddev b/w all types

                from (
                    select
                        count(*) as ct -- counts no. rows by type
                        from df -- accesses the temporary query table
                        group by Type -- aggregates over type

                )
        )
''').show()

#################################################################
# Query: Same Spread Statistics using inbuilt Functions Pyspark #
#################################################################

from pyspark.sql.functions import col, min, max, mean, stddev

df \
    .groupBy('Type') \
    .count() \
    .select([
        min(col("count")).alias('min'),
        max(col("count")).alias('max'),
        mean(col("count")).alias('mean'),
        stddev(col("count")).alias('stddev')
    ]) \
    .select([
        col('*'),
        (col("max") / col("min")).alias('minmaxratio')
    ]) \
    .show()

################################################
# Graph 2: Counts by Activity, ascending order #
################################################
# ordering on counts
counts = df.groupBy('Type').count().orderBy('count')

df_pandas = counts.toPandas()

# sorting values ascending
df_pandas = df_pandas.sort_values(by='Type', ascending=True)

# Create a horizontal bar plot
df_pandas.plot(kind='barh', x='Type', y='count', colormap='winter_r')
plt.show()

#################
# Undersampling #
#################
from pyspark.sql.functions import min

# creates disjoint non-overlapping classes in the dataset
types = [row[0] for row in df.select('Type').distinct().collect()]

# counts class elements, limits class samples
min = df.groupBy('Type').count().select(min('count')).first()[0]

# df outputted
df_balanced = None

# remember, classes are partitioned, non intersecting
for cls in types:

    # given current class_elements
    # shuffle (using fraction = 1.0 option for sample command)
    # returns only first n samples
    df_temp = df \
        .filter("Type = '"+cls+"'") \
        .sample(False, 1.0) \
        .limit(min)

    # assigning df_temp to empty df_balanced
    if df_balanced == None:
        df_balanced = df_temp

    # appending
    else:
        df_balanced=df_balanced.union(df_temp)

# Checking df_balanced class_elements

print(df_balanced.head())

df_balanced.groupBy('Type').count().show()

###################################################################

#####################
# Importing Dataset #
#####################

# reading and storing dataset
df = spark.read.csv('personal_data.csv', header="true")

# registering a corresponding query table / spark dataframe
df.createOrReplaceTempView('df')

#######################
# Checking row counts #
#######################
print(df.count())

############################
# Assigning to a Dataframe #
############################
# creating an apache spark sql dataframe
df.createOrReplaceTempView("df")

# querying that apache spark dataframe
spark.sql("SELECT * FROM df").show()


















# in order to display plot within window
# plt.show()
