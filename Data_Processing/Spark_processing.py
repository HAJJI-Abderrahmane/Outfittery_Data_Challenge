from pyspark.sql import SparkSession
from datetime import datetime,timedelta
import re
from sqlalchemy import create_engine
import logging
from pyspark.sql.functions import udf,col,monotonically_increasing_id, row_number
from pyspark.sql.types import StringType





def cleantags(x):
	#Using Regex to remove the tags and everything in between
	reg = re.compile('<.*?>') 
	cleaned_text = re.sub(reg, '', x)
	return cleaned_text
def body_transformation(x):
	#In case the body is not string we just return the old value
	if(isinstance(x, str)):
		return cleantags(x)
	else:
		return x

if __name__ == "__main__":
	# Create and configure logger
	logging.basicConfig(filename="spark_processing_logs.log",
	                    format='%(asctime)s %(message)s',
	                    filemode='a')
	logger = logging.getLogger()
	 
	# Setting the threshold of logger to DEBUG
	logger.setLevel(logging.DEBUG)

	#Initiating SparkSession
	spark = SparkSession.builder.enableHiveSupport().getOrCreate()

	dfposts = spark.read.format("xml").options(rowTag="row").load("../data/german.stackexchange.com/posts.xml")
	dfusers = spark.read.format("xml").options(rowTag="row").load("../data/german.stackexchange.com/Users.xml")

	##Cleaning & Transforming the data
	#all of the posts were scrapped with their tags, so first things first removing all html tags from the "Body" column
	udf_body =  udf(lambda x:body_transformation(x),StringType() )
	dfposts = dfposts.withColumn("_Body",udf_body(col("_Body")))
	#There are some rows with NaN values in _OwnerUserId in Posts that need to be removed
	#Spark reads OwnerUserId's type as INT so no need to cast it from float
	dfposts = dfposts.na.drop(subset=["_OwnerUserId"])
	#It also reads _CreationDate as timestamp data structure so no need to change it from STR.

	#Creating lists to create a column with later on
	Total_number_posts_list = []
	creation_date_list = []
	Number_posts_120days_list = []
	logger.info(f"Starting processing sequence")
	#Iterate over each User to create new needed columns
	dfusers.foreach(lambda row :
		logger.info(f"Processing User (Id:{row._Id})")
		#Total number of posts created
		Total_number_posts = dfposts.filter(f"_OwnerUserId=={row._Id}").count()

		#Creation date of the last post which has comments linked to it (Meaning _CommentCount is superior to 0)
		creation_date_list = dfposts.filter(f"_OwnerUserId=={row._Id} and _CommentCount>0").tail(1)
		if(len(creation_date_list)!=0):
			creation_date = creation_date_list[0]["_CreationDate"]
		else:
			creation_date = None # In case they dont have any posts
		#Number of posts in the last 30 days
		#We used 120 instead of 30 days for this prototype as None of the users has any Posts in the last 79days, 
		Number_posts_120days = dfposts.filter(f"_OwnerUserId == {row._Id} and _CreationDate> datetime.now() - timedelta(days=120)").count()

		#Appending the values to their respective lists
		Total_number_posts_list.append(Total_number_posts)
		creation_date_list.append(creation_date)
		Number_posts_120days_list.append(Number_posts_120days)
		logger.info(f"Finished Processing User (Id:{row.id})")
		)
	#Creating new columns
	b1 = spark.createDataFrame([(l,) for l in Total_number_posts_list], ['Total_number_posts'])
	b2 = spark.createDataFrame([(l,) for l in creation_date_list], ['creation_date'])
	b3 = spark.createDataFrame([(l,) for l in Number_posts_120days_list], ['Number_posts_120days'])

	b1 = b1.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
	b2 = b2.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))
	b3 = b3.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))

	dfusers = dfusers.withColumn("row_idx", row_number().over(Window.orderBy(monotonically_increasing_id())))

	dfusers = dfusers.join(b1, dfusers.row_idx == b1.row_idx)
	dfusers = dfusers.join(b2, dfusers.row_idx == b2.row_idx)
	dfusers = dfusers.join(b3, dfusers.row_idx == b3.row_idx).drop("row_idx")
	#A mysql server should be running on localhost with port 3306 with "test_bigdata" database for this script to send the data to the database
	logger.info(f"Sending Data to mysql Server...")
	dfusers.write.format('jdbc').options(
	      url='jdbc:mysql://localhost/test_bigdata',
	      driver='com.mysql.jdbc.Driver',
	      dbtable='Users',
	      user='root',
	      password='').mode('append').save()
	logger.info(f"Task Done")
	
	##Sending data to apache Kafka's broker
	#ds = dfusers.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream.format("kafka") \
	#  .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
	#  .option("topic", "Analytics") \
	#  .start()

	##Writting data to Hive database
	#dfuser.write.mode("append").saveAsTable("test_bigdata.Users")
