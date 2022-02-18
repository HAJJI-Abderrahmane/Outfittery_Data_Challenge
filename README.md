# Outfittery Data Challenge

Hello, in this repo, i'll try to tackle Outfittery's Data Challenge in a way that can be scalable and can also be implemented in the cloud while also retaining crucial objectives like :
 - Fault tolerance.
 - Realtime latency.
 - Consistency.
 > The following diagram explains the system design :
 ```mermaid
graph LR
A[Data source ] -- HTTPS --> B[Data Staging ]
B --HDFS--> D[Data Processing]
B --cloudStorage S3--> D[Data Processing]
D --TCP--> E{Message broker}
E --> F[Relational Database]
D --TCP--> F
```
- **Data Source :** It's located in archive.org and it has to be retrieved over HTTPS protocol.
- **Data Staging :** Scraps the website (stackexchange) and downloads the files, as well as extract them if need be.

- **Data Processing :** Reads the **.xml** files and do the necessary processing to output a table to either the message broker or directely to the database.
- **Message broker:** Allow us to decouple the processes and services as well as validate the messages, very crucial in case of multiple data sources and data warehouses.
-**Relational Database:** Any sql dabase would suffice like : **mariaDB, OracleDB, PostgreSQL...**
## Running the prototype


A prototype can be run by running the following steps:
- installing necessary libraries on python by : `'pip install requirements.txt'`
- running : `'start-script.bat'`  
> Might take a while depending on your download speed of the 54mb of the [german.stackexchange.com.7z](https://archive.org/download/stackexchange/german.stackexchange.com.7z)

It will download [german.stackexchange.com.7z](https://archive.org/download/stackexchange/german.stackexchange.com.7z) , extract it *(only Posts.xml,Comments.xml,Users.xml )*  <br /> read it with **pandas**, process it *(by creating new columns to the Users table)*, and sending the data to a mysql server.
