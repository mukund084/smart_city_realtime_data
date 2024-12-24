from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import from_json, col
from config import configuration

def main():
    # Create SparkSession with necessary dependencies for Kafka & S3
    spark = SparkSession.builder \
        .appName("SmartCityStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()
        
    # Adjust the log level to minimize console messages
    spark.sparkContext.setLogLevel('WARN')
    
    # Define a vehicle schema as an example
    vehicleSchema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("deviceid", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("speed", DoubleType(), nullable=True),
        StructField("direction", StringType(), nullable=True),
        StructField("make", StringType(), nullable=True),
        StructField("model", StringType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("fuelType ", StringType(), nullable=True)
    ])
    
    gpsSchema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("deviceid", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("speed", DoubleType(), nullable=True),
        StructField("direction", StringType(), nullable=True),
        StructField("vehicle_type", StringType(), nullable=True)
    ])
    
    traffic_cameraSchema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("deviceid", StringType(), nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("cameraId", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("snapshot", StringType(), nullable=True)
    ])
    
    weatherSchema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("deviceid", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("temperature", DoubleType(), nullable=True),
        StructField("weatherCondition", StringType(), nullable=True),
        StructField("windSpeed", DoubleType(), nullable=True),
        StructField("humidity", DoubleType(), nullable=True),
        StructField("airQualityIndex", DoubleType(), nullable=True)
    ])
    
    
    emergencySchema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("deviceid", StringType(), nullable=True),
        StructField("incidentId", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True)
    ])
    
    
        
    def read_kafka_topic(topic, schema):
        return (
            spark.readStream
                .format('kafka')
                .option("kafka.bootstrap.servers", "broker:29092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                .withWatermark("timestamp", "2 minutes")
        )

        
    def streamWriter(df, checkpointFolder, output):
        return (df.writeStream
                .format('parquet')
                .option("checkpointLocation", checkpointFolder)
                .option("path", output)
                .outputMode('append')
                .start()
                )

    
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', traffic_cameraSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    #join all the dfs with id and timestamps
    
    query_1 = streamWriter(vehicleDF, 's3a://spark-streaming-data-project-2024/checkpoints/vehicle_data', 
                            's3a://spark-streaming-data-project-2024/data/vehicle_data')
    
    query_2 = streamWriter(gpsDF, 's3a://spark-streaming-data-project-2024/checkpoints/gps_data', 
                        's3a://spark-streaming-data-project-2024/data/gps_data')
    
    query_3 = streamWriter(trafficDF, 's3a://spark-streaming-data-project-2024/checkpoints/traffic_data', 
                        's3a://spark-streaming-data-project-2024/data/traffic_data')
    
    query_4 = streamWriter(weatherDF, 's3a://spark-streaming-data-project-2024/checkpoints/weather_data', 
                        's3a://spark-streaming-data-project-2024/data/weather_data')

    query_5 = streamWriter(emergencyDF, 's3a://spark-streaming-data-project-2024/checkpoints/emergency_data', 
                        's3a://spark-streaming-data-project-2024/data/emergency_data')
    
    query_5.awaitTermination()
    


if __name__ == "__main__":
    main()