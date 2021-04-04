
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.window import Window
from time import time

#Task1

st = time()

# spark = SparkSession \
#     .builder \
#     .appName("Python Spark SQL basic example") \
#     .config("spark.jars", "postgresql-42.2.19.jar") \
#     .config("spark.num.executors", 2)\
#     .getOrCreate()


# jdbcDF = spark.read.format("jdbc"). \
# options(
#          url='jdbc:postgresql://localhost:5432/postgres', # jdbc:postgresql://<host>:<port>/<database>
#          dbtable='pullreq',
#          user='sushantverma',
#          password='sushantabc',
#          driver='org.postgresql.Driver').\
# load()
#
#
#
# jdbcDF.where(func.col('status') == 'opened').groupby(func.to_date('timestamp')).count()\
# .show()


# 2
# jdbcDF.where(func.col('status') == 'discussed').groupby(func.to_date('timestamp')).count()\
# .show();

# 3
# sub = jdbcDF.where(func.col('status') == 'discussed').groupBy(func.year('timestamp'), func.month('timestamp'), 'username').count()
# w = Window.partitionBy('year(timestamp)', 'month(timestamp)')
# sub1 = sub.withColumn('max_cnt', func.max('count').over(w)).where(func.col('count') == func.col('max_cnt')).drop('max_cnt').orderBy('year(timestamp)', 'month(timestamp)', ascending=False)
# sub1.show()

# 4
# sub = jdbcDF.where(func.col('status') == 'discussed').groupBy(func.year('timestamp'), func.weekofyear('timestamp'), 'username').count()
# w = Window.partitionBy('year(timestamp)', 'weekofyear(timestamp)')
# sub1 = sub.withColumn('max_cnt', func.max('count').over(w)).where(func.col('count') == func.col('max_cnt')).drop('max_cnt').orderBy('year(timestamp)', 'weekofyear(timestamp)', ascending=False)
# sub1.show()

# 5
# jdbcDF.where(func.col('status') == 'opened').groupBy(func.year('timestamp'), func.weekofyear('timestamp')).count().orderBy(func.year('timestamp'), func.weekofyear('timestamp'))\
# .show()

# 6
# jdbcDF.where((func.col('status') == 'merged') & (func.year('timestamp') == 2010)).groupBy(func.month('timestamp')).count().orderBy(func.month('timestamp'))\
# .show()

# 7
# jdbcDF.orderBy(func.to_date('timestamp')).groupBy(func.to_date('timestamp')).count()\
# .show()

# 8
# jdbcDF.where((func.col('status') == 'opened') & (func.year('timestamp') == 2011)).groupBy('username').count().orderBy('count', ascending=False).limit(1)\
# .show()





#Task2


# import pymongo , csv
# from datetime import  datetime
#
# myclient = pymongo.MongoClient("mongodb://localhost:27017/")
# mydb = myclient["sushant"]
# mycol = mydb["pullreq"]
#
# headers = ["seq" , "username" , "status" , "timestamp"]
# with open('pullreq_events.csv') as csv_file:
#     csv_reader = csv.reader(csv_file, delimiter=',')
#     cnt = 0;
#     for row in csv_reader:
#         to_insert = {}
#         to_insert["seq"] = int(row[0])
#         to_insert["username"] = row[1]
#         to_insert["status"] = row[2]
#         to_insert["timestamp"] = datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S')
#         mycol.insert_one(to_insert)

# #


# ans  = mycol.aggregate([
#     {
#         "$project":{
#             "status" : "opened",
#             "timestamp" : 1
#         }
#     },
#    { "$group": { "_id": { "$toDate": "$timestamp"},
#                "count": { "$sum": 1 } }
#     },
#     {
#         "$sort": {"count": -1}
#     }
#    ])
#
#
# ans  = mycol.aggregate([
#     {
#         "$project":{
#             "status" : "discussed",
#             "timestamp" : 1
#         }
#     },
#    { "$group": { "_id": { "$toDate": "$timestamp"},
#                "count": { "$sum": 1 } }
#     },
#     {
#         "$sort": {"count": -1}
#     }
#    ])
#
# ans  = mycol.aggregate([
#     {
#         "$project":{
#             "status" : "opened",
#             "timestamp" : 1
#         }
#     },
#    { "$group": { "_id": { "$week": "$timestamp"},
#                "count": { "$sum": 1 } }
#     },
#     {
#         "$sort": {"count": -1}
#     }
#    ])
#
#
#
# cnt = 0;
# for x in ans:
#     print(x)
#     cnt+=1
#     if(cnt == 10):
#         break


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/sushant.pullreq") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/sushant.pullreq") \
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .config("spark.num.executors", 1)\
    .getOrCreate()

jdbcDF = my_spark.read.format("mongo").load()

#1
# jdbcDF.where(func.col('status') == 'opened').groupby(func.to_date('timestamp')).count()\
# .show()


# 2
# jdbcDF.where(func.col('status') == 'discussed').groupby(func.to_date('timestamp')).count()\
# .show();

# 3
# sub = jdbcDF.where(func.col('status') == 'discussed').groupBy(func.year('timestamp'), func.month('timestamp'), 'username').count()
# w = Window.partitionBy('year(timestamp)', 'month(timestamp)')
# sub1 = sub.withColumn('max_cnt', func.max('count').over(w)).where(func.col('count') == func.col('max_cnt')).drop('max_cnt').orderBy('year(timestamp)', 'month(timestamp)', ascending=False)
# sub1.show()

# 4
# sub = jdbcDF.where(func.col('status') == 'discussed').groupBy(func.year('timestamp'), func.weekofyear('timestamp'), 'username').count()
# w = Window.partitionBy('year(timestamp)', 'weekofyear(timestamp)')
# sub1 = sub.withColumn('max_cnt', func.max('count').over(w)).where(func.col('count') == func.col('max_cnt')).drop('max_cnt').orderBy('year(timestamp)', 'weekofyear(timestamp)', ascending=False)
# sub1.show()

# 5
# jdbcDF.where(func.col('status') == 'opened').groupBy(func.year('timestamp'), func.weekofyear('timestamp')).count().orderBy(func.year('timestamp'), func.weekofyear('timestamp'))\
# .show()

# 6
jdbcDF.where((func.col('status') == 'merged') & (func.year('timestamp') == 2012)).groupBy(func.month('timestamp')).count().orderBy(func.month('timestamp'))\
.show()

#7
# jdbcDF.orderBy(func.to_date('timestamp')).groupBy(func.to_date('timestamp')).count()\
# .show()

# 8
jdbcDF.where((func.col('status') == 'opened') & (func.year('timestamp') == 2011)).groupBy('username').count().orderBy('count', ascending=False).limit(1)\
.show()


# st = time()
# #
# spark = SparkSession \
#     .builder \
#     .appName("Python Spark SQL basic example") \
#     .config("spark.jars", "postgresql-42.2.19.jar") \
#     .config("spark.num.executors", 1)\
#     .getOrCreate()


# jdbcDF = spark.read.format("jdbc"). \
# options(
#          url='jdbc:postgresql://localhost:5432/postgres', # jdbc:postgresql://<host>:<port>/<database>
#          dbtable="(select timestamp::date, count(*) from pullreq where status = 'opened' group by timestamp::date) as blah",
#          user='sushantverma',
#          password='sushantabc',
#          driver='org.postgresql.Driver').\
# load()


# jdbcDF = spark.read.format("jdbc"). \
# options(
#          url='jdbc:postgresql://localhost:5432/postgres', # jdbc:postgresql://<host>:<port>/<database>
#          dbtable="(select timestamp::date, count(*) from pullreq where status = 'discussed' group by timestamp::date) as blah",
#          user='sushantverma',
#          password='sushantabc',
#          driver='org.postgresql.Driver').\
# load()

# jdbcDF = spark.read.format("jdbc"). \
# options(
#          url='jdbc:postgresql://localhost:5432/postgres', # jdbc:postgresql://<host>:<port>/<database>
#          dbtable="( select * from (select year , month , username , cnt, RANK() OVER (PARTITION BY year,month ORDER BY cnt DESC) AS rn from ( select extract(year from timestamp)  as year, extract(month from timestamp) as month, username                      as username, count(*)                      as cnt from pullreq where (status = 'discussed') group by year, month, username ) as t1 ) as t2 where t2.rn = 1 order by year desc , month desc) as blah",
#          user='sushantverma',
#          password='sushantabc',
#          driver='org.postgresql.Driver').\
# load()


# jdbcDF = spark.read.format("jdbc"). \
# options(
#          url='jdbc:postgresql://localhost:5432/postgres', # jdbc:postgresql://<host>:<port>/<database>
#          dbtable="( select * from (select year , week , username , cnt, RANK() OVER (PARTITION BY year,week ORDER BY cnt DESC) AS rn from ( select extract(year from timestamp)  as year, extract(week from timestamp) as week, username                      as username, count(*)                      as cnt from pullreq where (status = 'discussed') group by year, week, username ) as t1 ) as t2 where t2.rn = 1 order by year desc , week desc) as blah",
#          user='sushantverma',
#          password='sushantabc',
#          driver='org.postgresql.Driver').\
# load()

# jdbcDF = spark.read.format("jdbc"). \
# options(
#          url='jdbc:postgresql://localhost:5432/postgres', # jdbc:postgresql://<host>:<port>/<database>
#          dbtable="(select extract(year from timestamp) as year, extract(week from timestamp) as week, count(*) as count from pullreq where status = 'opened' group by extract(year from timestamp), extract(week from timestamp) order by extract(year from timestamp), extract(week from timestamp) asc) as blah",
#          user='sushantverma',
#          password='sushantabc',
#          driver='org.postgresql.Driver').\
# load()


# jdbcDF = spark.read.format("jdbc"). \
# options(
#          url='jdbc:postgresql://localhost:5432/postgres', # jdbc:postgresql://<host>:<port>/<database>
#          dbtable="(select extract(month from timestamp) as month, count(*) as count from pullreq where status = 'merged' and extract(year from timestamp) = 2010 group by extract(month from timestamp) order by extract(month from timestamp)) as blah",
#          user='sushantverma',
#          password='sushantabc',
#          driver='org.postgresql.Driver').\
# load()

# jdbcDF = spark.read.format("jdbc"). \
# options(
#          url='jdbc:postgresql://localhost:5432/postgres', # jdbc:postgresql://<host>:<port>/<database>
#          dbtable="(select date(timestamp), count(*) as num_events from pullreq group by date(timestamp) order by date(timestamp) asc) as blah",
#          user='sushantverma',
#          password='sushantabc',
#          driver='org.postgresql.Driver').\
# load()

# jdbcDF = spark.read.format("jdbc"). \
# options(
#          url='jdbc:postgresql://localhost:5432/postgres', # jdbc:postgresql://<host>:<port>/<database>
#          dbtable="(select username,count(*) as num from pullreq where extract(year from timestamp) = 2011 AND status = 'opened' group by username order by num desc  limit 1) as blah",
#          user='sushantverma',
#          password='sushantabc',
#          driver='org.postgresql.Driver').\
# load()

# jdbcDF.show()

end = time()

print(end - st)



