# Running the project

In order to fulfill the tasks of the project, the start-up commands given in the project tasks had to be changed to ensure a successful run:
    `/usr/bin/zookeeper-server-start config/zookeeper.properties` <br>
    `/usr/bin/kafka-server-start config/server.properties`
    
# Question 1
> How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

In order to quantify throughput and latency, I chose two variables in the console progress report, as presented in the course content: `inputRowsPerSecond` for throughput and `processedRowsPerSecond` for latency. In order to select a small set of SparkSession parameters to maximize the impact on the job performance, I used these sources:
* [Tuning Spark SQL](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
* [Tuning Spark Streaming](https://spark.apache.org/docs/2.1.1/streaming-programming-guide.html)
* [Spark Streaming configuration docs](https://spark.apache.org/docs/2.1.1/configuration.html)

According to these resources, I extracted the following parameters that improved both processing time and throughput in Spark Streaming and Spark SQL:
* `spark.sql.shuffle.partitions` - since this affects shuffling for joins and aggregations (main scope of our app), it improves processing time
* `spark.sql.files.minPartitionNum` - useful for file-based sources (we used JSON); it indicates how much the input file should be split to improve parallelism
* `spark.streaming.backpressure.enabled` - this parameter allows dynamically setting the message reception rate so that processing is finished before new batches arrive. Setting this parameter offered more flexibility than just tuning `spark.streaming.kafka.maxRatePerPartition`, thus improving throughput, as well as processing time.

# Question 2
> What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

In order to perform SparkSession parameter tuning, I initially used the Jobs and Stages tabs in the Spark UI. Based on these, I noticed that the executor time was the biggest chunk of all the stages. Since, the main scopes of the application are aggregations and joins, I firstly tuned the first two parameters I mentioned in Question 1: `spark.sql.shuffle.partitions`, and `spark.sql.files.minPartitionNum`.

In order to evaluate the performance of a specific set of parameters I looked at an average of the two metrics -- inputRowsPerSecond and processedRowsPerSecond -- from the console progress report of the first few jobs. Additionally, I selected a set of parameters in such a way that the inputRows and processedRows weren't vastly different so that the application consumes as much as it can process.

| shuffle.partitions | minPartitionNum | inputRowsPerSecond | processedRowsPerSecond |
|:------------------:|:---------------:|:----------------------:|:----------------------:|
| 100 |  200 | 13 | 40 |
| 200 |  200 | 10 | 24 |
| 200 |  300 | 23 | 28 |
| **200** |  **400** | **24** | **28** |
| 300 |  400 | 8  | 18 |

Based on the table and the two metrics mentioned above, I chose to set `spark.sql.shuffle.partitions` = 200 and `spark.sql.files.minPartitionNum` = 400, which was a vast improvement compared to the baseline. Without any tuning, my metrics were around 7 for inputRowsPerSecond and around 15 for processedRowsPerSecond.

Next, I further improved both throughput and latency by setting `spark.streaming.backpressure.enabled` = true.

| shuffle.partitions | minPartitionNum | backpressure | inputRowsPerSecond | processedRowsPerSecond |
|:------------------:|:---------------:|:------------:|:----------------------:|:----------------------:|
| 200 |  400 | true  | 28 | 31 |