"""
Pandas implementation of the prototype for the processing of data and sending it to a mysql server.
"""
import pandas as pd
from datetime import datetime
import re,sys
from sqlalchemy import create_engine
import logging

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
	logging.basicConfig(filename="pandas_processing_logs.log",
	                    format='%(asctime)s %(message)s',
	                    filemode='a')
	logger = logging.getLogger()
	 
	# Setting the threshold of logger to DEBUG
	logger.setLevel(logging.DEBUG)

	url = sys.argv[1]

	exchangename = url.split("/")[-1][:-3]
	print(url)
	print(exchangename)
	dfposts = pd.read_xml("data/"+str(exchangename)+"/Posts.xml") ###Should install lxml for pandas to read xml 
	dfusers = pd.read_xml("data/"+str(exchangename)+"/Users.xml")

	##Cleaning & Transforming the data
	#all of the posts were scrapped with their tags, so first things first removing all html tags from the "Body" column
	dfposts.Body = dfposts.Body.apply(lambda x: body_transformation(x))
	#There are some rows with NaN values in OwnerUserId in Posts that need to be removed, as well as convert them from 
	#float to int as float takes more space and its pointless to have them as float since IDs are integers.
	dfposts = dfposts[dfposts.OwnerUserId.notna()]
	dfposts.OwnerUserId = dfposts.OwnerUserId.astype(int)
	#Convert CreationDate from string to datetime format for easier handling while processing
	dfposts.CreationDate = pd.to_datetime(dfposts.CreationDate)

	#Creating lists to create a column with later on
	Total_number_posts_list = []
	creation_date_list = []
	Number_posts_120days_list = []

	logger.info(f"Starting processing sequence")
	#Iterate over each User to create new needed columns
	for _,row in dfusers.iterrows():
		logger.info(f"Processing User (Id:{row.Id})")
		#Total number of posts created
		Total_number_posts = dfposts[dfposts.OwnerUserId==float(row.Id)].shape[0]

		#Creation date of the last post which has comments linked to it (Meaning CommentCount is superior to 0)
		creation_date_df = dfposts[dfposts.eval(f"OwnerUserId=={float(row.Id)} & CommentCount>0")]
		if(creation_date_df.shape[0]!=0):
			creation_date = creation_date_df.iloc[-1].CreationDate
		else:
			creation_date = None # In case they dont have any posts

		#Number of posts in the last 30 days
		#We used 120 instead of 30 days for this prototype as None of the users has any Posts in the last 79days, 
		Number_posts_120days = dfposts[(dfposts.CreationDate>(datetime.now()-pd.Timedelta(days=120)))&(dfposts.OwnerUserId==float(row.Id))].shape[0]

		#Appending the values to their respective lists
		Total_number_posts_list.append(Total_number_posts)
		creation_date_list.append(creation_date)
		Number_posts_120days_list.append(Number_posts_120days)
		logger.info(f"Finished Processing User (Id:{row.Id})")
	#Creating new columns
	dfusers["Total_number_of_posts_created"] = Total_number_posts_list
	dfusers["Creation_date_of_the_last_post"] = creation_date_list
	dfusers["Number_of_posts_in_the_last_120_days"] = Number_posts_120days_list

	#A mysql server should be running on localhost with port 3306 with "test_bigdata" database for this script to send the data to the database
	logger.info(f"Connecting to mysql Server...")
	engine = create_engine("mysql://root:@localhost:3306/test_bigdata")
	logger.info(f"Sending Data to mysql Server...")
	dfusers.to_sql('Users', con=engine, if_exists='append')
	logger.info(f"Task Done")
	#this can be done to the others if need be (Comments,Votes...)
