"""
Ingestion & Extraction of data 
"""
import requests
import threading,queue
import logging
import sys,os,re
import py7zr
from bs4 import BeautifulSoup

 


def download_extract(url):
	"""
	input : url to download
	ouput : Extracted comments,users,posts .xml to their respective folders
	"""
	os.makedirs("data",exist_ok=True)
	if(url.split("/")[-1][-3:]==".7z"):
		response = requests.get(url, allow_redirects=True)
		filter_pattern = re.compile(r'(Comments.xml|Posts.xml|Users.xml)')
		os.makedirs("/data/"+url.split("/")[-1][:-3],exist_ok=True)
		open("data/"+url.split("/")[-1], 'wb').write(response.content) # Writing it to disk so we won't have Memory problems incase these files are huge (TB).
		archive = py7zr.SevenZipFile("data/"+url.split("/")[-1], mode='r')
		allfiles = archive.getnames()
		selective_files = [f for f in allfiles if filter_pattern.match(f)]
		archive.extract(targets=selective_files,path="data/"+url.split("/")[-1][:-3]+"/")
		archive.close()

		os.remove("data/"+url.split("/")[-1])




def worker(q):
    while True:
        item = q.get()
        print(f'Working on {item}')
        logger.info(f'Working on {item}')
        download_extract(item)
        logger.info(f'Finished on {item}')
        q.task_done()

def queue_thread(nthread):
	q = queue.Queue() #Creating a FIFO queue for thread number n for every nth element with a nthread offset

	
	# turn-on the worker on nth thread
	threading.Thread(target=worker, daemon=True,args=[q]).start()
	for url_index in range(number_of_threads-nthread,url_list_length-nthread,number_of_threads):
		q.put(url_list[url_index])
	#Putting "leftover" url elements in the first thread
	if(nthread==0):
		q.put(url_list[0])
		for url_index in range(url_list_length-((url_list_length-1)%number_of_threads),url_list_length):
			q.put(url_list[url_index])

	# block until all tasks are done
	q.join()


if __name__ == "__main__":
	url = 'https://archive.org/download/stackexchange/'
	# Create and configure logger
	logging.basicConfig(filename="staging_logs.log",
	                    format='%(asctime)s %(message)s',
	                    filemode='a')
	logger = logging.getLogger()
	 
	# Setting the threshold of logger to DEBUG
	logger.setLevel(logging.DEBUG)


	## Scraping urls of all data available using beautifulsoup4
	r = requests.get(url, allow_redirects=True)
	soup = BeautifulSoup(r.text, "html.parser" )
	mytds = soup.findAll('td')
	url_list=[]
	for tr in mytds[3::3]: #We only take every 3rd (and avoid the first 3) td tags as 2 td tags are for last modified date and file size
		filename = tr.text.split(" ")[0] #Extract the filename and remove the redundant information
		url_list.append(url+filename)

	if(len(sys.argv)==1):
		number_of_threads = 1 #incase no argument was given
	else:
		number_of_threads = int(sys.argv[1])
	url_list_length = len(url_list)
	logger.info(f'Number of threads : {number_of_threads}' )
	#Creating a data folder to put the data in 
	os.makedirs("data",exist_ok=True)

	for nthread in range(number_of_threads):
		#starting a thread to queue up the nth element 
		threading.Thread(target=queue_thread,args=[nthread]).start()

	
	logger.info('All work completed')


