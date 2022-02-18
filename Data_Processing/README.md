# Data Processing

There are many ways to process data, for our working prototype, we used **pandas** library to do most of what we need, but in production cases, which might have data size ranging from gb to tb, **pandas** might not be the optimal solution, so i also wrote a standalone script that uses **Apache Spark** instead, meaning the processing can be distributed and scaled over many machines *(databricks in the cloud)*.

 Running the standalone script *(Either the pandas or spark version)* will read the data, transforms it, and sends it to a mysql server.
 
 Ideally the data would be sent to a message broker like **Apache Kafka** *(imlemented the code in the spark version code)* and have a database like **Hive** which can subscribe to Kafka's topics to receive the data.

Another approach would be to treat this as a streaming pipeline if the data sources were constantly updating the data, in that case **Spark Streaming** or **Apache Storm** would be perfect for the task.
## Running the standalone Pandas version

### Prerequisites:
- `libraries in requirements.txt `
- `the data(german.stackexchange.com) exists in /data/german.stackexchange.com`
### Running it
`python Pandas_processing.py`

## Running the standalone Spark version

### Prerequisites:
- `pyspark installed and can be ran. `
- `libraries in requirements.txt `
- `the data(german.stackexchange.com) exists in /data/german.stackexchange.com`
### Running it
`spark-submit --packages com.databricks:spark-xml_2.12:0.14.0 Spark_processing.py`