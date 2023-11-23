import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

logging.basicConfig(level=logging.ERROR)

def get_spark():
    # необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
    spark_jars_packages = ",".join(
            [
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
                "org.postgresql:postgresql:42.4.0",
            ]
        )

    # создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
    spark = SparkSession.builder \
        .appName("FinalProject") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.adaptive.enabled", False) \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
        
    return spark

def transaction_stream_df_get(spark):
    try:
        # читаем из топика Kafka сообщения с акциями от ресторанов 
        transaction_df = (
            spark.read
            .format('kafka')
            .option('kafka.bootstrap.servers', 'rc1a-f179p9ij8hivfed1.mdb.yandexcloud.net:9091')
            .option('kafka.security.protocol', 'SASL_SSL')
            .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="producer_consumer" password="1q2w3e4r5t";') \
            .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
            .option('kafka.ssl.ca.location', '/root/.ssh/YandexInternalRootCA.crt')
            .option('kafka.sasl.username', 'producer_comsumer')
            .option('kafka.sasl.password', '1q2w3e4r5t')
            .option('subscribe', 'transaction-service-input')
            .option("startingOffsets", "earliest")
            .load()
        )
        
        # определяем схему входного сообщения для json
        message_schema = StructType(
            [
                StructField("object_id" , StringType(), True),
                StructField("object_type", StringType(), True),
                StructField("sent_dttm" , TimestampType(), True),
                StructField("payload" , StringType(), True)
            ]
        )

        transaction_stream_df = (
                transaction_df
                .withColumn('value', f.col('value').cast(StringType()))
                .withColumn('data', f.from_json(f.col('value'), message_schema))
                .withColumn('object_id', f.col('data.object_id').cast(StringType()))
                .withColumn('object_type', f.col('data.object_type').cast(StringType()))
                .withColumn('sent_dttm', f.col('data.sent_dttm').cast(TimestampType()))
                .withColumn('payload', f.col('data.payload'))
                .select('object_id', 
                        'object_type', 
                        'sent_dttm', 
                        'payload'
                        )
                .where(~f.col('object_id').isNull())
                .dropDuplicates(['object_id'])
                .withWatermark('sent_dttm', '5 minutes')
            )
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")  
         
    return transaction_stream_df


if __name__ == "__main__":
    spark = get_spark()
    transaction_stream_df = transaction_stream_df_get(spark)
    transaction_stream_df.printSchema()

    (
        transaction_stream_df
        .write
        .mode("append")
        .format('jdbc')
        .option('url', 'jdbc:postgresql://rc1b-exusdl5kvwaak3bg.mdb.yandexcloud.net:6432/final')
        .option('driver', 'org.postgresql.Driver')
        .option('dbtable', 'stg.transaction_service')
        .option('user', 'db_user')
        .option('password', '1q2w3e4r5t')
        .save()
    )
    
