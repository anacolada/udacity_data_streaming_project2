import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType()),
    StructField("original_crime_type_name", StringType()),
    StructField("report_date", StringType()),
    StructField("call_date", StringType()),
    StructField("offense_date", StringType()),
    StructField("call_time", StringType()),
    StructField("call_date_time", TimestampType()),
    StructField("disposition", StringType()),
    StructField("address", StringType()),
    StructField("state", StringType()),
    StructField("agency_id", StringType()),
    StructField("address_type", StringType()),
    StructField("common_location", StringType()),\
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "org.sanfranciscopolice.crime") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "200") \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("cast(value as string)")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF")) \
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    # according to: https://knowledge.udacity.com/questions/349349
    distinct_table = service_table \
                     .select("original_crime_type_name", "disposition", "call_date_time") \
                     .withWatermark("call_date_time", "60 minute")
    
#     distinct_table.printSchema()
#     |-- call_date_time: timestamp (nullable = true)
    
    # count the number of original crime type
    w = psf.window("call_date_time", "30 minutes", "10 minutes")
    agg_df = distinct_table \
             .select("original_crime_type_name", "call_date_time", "disposition") \
             .withWatermark("call_date_time", "60 minutes") \
             .groupby(w, "original_crime_type_name") \
             .agg(psf.count("original_crime_type_name").alias("count_crime_type")) \
             .sort(psf.col("count_crime_type").desc())
    
    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df \
        .writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode("Complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # TODO attach a ProgressReporter
    query.awaitTermination()
    
    
    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # TODO join on disposition column
    join_query = agg_df.join(radio_code_df, ['disposition'])

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .getOrCreate()
    
    # improve processing
    spark.conf.set("spark.sql.shuffle.partitions", 200)
    spark.conf.set("spark.sql.files.minPartitionNum", 400)
    
    # improve throughput (and processing)
    spark.conf.set("spark.streaming.backpressure.enabled", "true")
    #spark.conf.set("spark.streaming.kafka.maxRatePerPartition", 200)
    #spark.conf.set("spark.streaming.ui.retainedBatches", 200)
    
    #spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
