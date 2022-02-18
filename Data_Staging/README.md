# Data Staging

In this step, the data has to be quickly and autonomously secured, in this case, archive.org allows only 200-300kb/s of bandwith per request which will take a long time if we want all the data in all the exchanges in stackexchange.<br />
I used a multi-producer, multi-consumer approach that creates as many workers as possible and downloads/extracts the **.7z** files following a FIFO priority queue for each worker, this was implemented to solve threading issues, and ensure consistency. This solution is scalable horizontally as more machines added, more threads can be created.<br />
This can be ran independently as a standalone script by running it :
>python Staging.py N:*number_of_threads* *(i.e python Staging 16)*

It will go through [stackexchange](https://archive.org/download/stackexchange), get all the downloadable urls,deploy `N` threads split evenly on all the urls,then launch them simultaneously to start the download, once finished, extract them to a newly created `data` folder, and create for each exchange their respective folder inside data, and place the data inside of it. *(for this prototype, we only extract Posts.xml,Comments.xml,Users.xml)*

There are still things that can still be implemented in this step, like implementing a workflow manager *(like **apache Airflow**)* that can monitor data sources and schedule downloads and extractions of the newly added data.
