import os

from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType


TOPIC_NAME_IN = 'ibndaud_in'
TOPIC_NAME_OUT = 'ibndaud_out'

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

postgresql_settings_in = {
    'user': 'student',
    'password': 'de-student'
}

postgresql_settings_out = {
    'user': 'jovyan',
    'password': 'jovyan'
}


def spark_init(session_name) -> SparkSession:
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )
    spark = (SparkSession.builder.appName(session_name)\
             .config("spark.sql.session.timeZone", "UTC")\
                .config("spark.jars.packages", spark_jars_packages)\
                    .getOrCreate()
                    )
    return spark


def restaurant_read_stream(spark: SparkSession) -> DataFrame:
    incomming_message_schema = StructType([StructField('restaurant_id', StringType(), True), 
                                           StructField('adv_campaign_id', StringType(), True), 
                                           StructField('adv_campaign_content', StringType(), True), 
                                           StructField('adv_campaign_owner', StringType(), True), 
                                           StructField('adv_campaign_owner_contact', StringType(), True), 
                                           StructField('adv_campaign_datetime_start', LongType(), True), 
                                           StructField('adv_campaign_datetime_end', LongType(), True), 
                                           StructField('datetime_created', LongType(), True)]
                                           )
    current_timestamp_utc = int(datetime.now(timezone.utc).timestamp())
    df = (spark.readStream.format('kafka')\
          .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')\
            .options(**kafka_security_options)\
                .option("subscribe", TOPIC_NAME_IN)\
                    .load()\
                        .withColumn('value', F.col('value').cast(StringType()))\
                            .withColumn('parsed_key_value', F.from_json(F.col('value'), incomming_message_schema))\
                                .selectExpr('parsed_key_value.*')\
                                    .filter((F.col('parsed_key_value.adv_campaign_datetime_start') < 
                                             F.lit(current_timestamp_utc)) & (
                                                 F.col('parsed_key_value.adv_campaign_datetime_end') > 
                                                 F.lit(current_timestamp_utc)))
                                                 )
    return df


def subscribers_restaurant_read(spark: SparkSession) -> DataFrame:
    df = (spark.read.format('jdbc')\
          .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de')\
            .option('driver', 'org.postgresql.Driver')\
                .option('dbtable', 'subscribers_restaurants')\
                    .options(**postgresql_settings_in)\
                        .load())
    return df


def join_stream_subscribers(stream_df, subscribers_df) -> DataFrame:
    current_timestamp_utc = int(datetime.now(timezone.utc).timestamp())
    df = (stream_df.join(subscribers_df, 'restaurant_id', 'inner')\
          .select(F.col('restaurant_id'), F.col('adv_campaign_id'), F.col('adv_campaign_content'), 
                  F.col('adv_campaign_owner'), F.col('adv_campaign_owner_contact'), 
                  F.col('adv_campaign_datetime_start'), F.col('adv_campaign_datetime_end'), 
                  F.col('datetime_created'), F.col('client_id'))\
                    .withcolumn('trigger_datetime_created', F.lit(current_timestamp_utc)))
    return df


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    # записываем df в PostgreSQL с полем feedback
    df.write.mode('append')\
        .format('jdbc')\
            .option('url', 'jdbc:postgresql://localhost:5432/')\
                .option('driver', 'org.postgresql.Driver')\
                    .option('dbtable', 'subscribers_feedback')\
                        .options(**postgresql_settings_out)\
                            .save()
    # создаём df для отправки в Kafka. Сериализация в json.
    kafka_df = df.select(F.to_json(F.struct('restaurant_id', 'adv_campaign_id', 'adv_campaign_content', 
                                            'adv_campaign_owner', 'adv_campaign_owner_contact', 
                                            'adv_campaign_datetime_start', 'adv_campaign_datetime_end', 
                                            'client_id', 'datetime_created', 'trigger_datetime_created'))\
                                                .alias('value'))
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    kafka_df.write.format('kafka')\
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')\
            .options(**kafka_security_options)\
                .option('topic', TOPIC_NAME_OUT)\
                    .save()
    # очищаем память от df
    df.unpersist()


spark = spark_init('RestaurantSubscribeStreamingService')
restaurant_read_stream_df = restaurant_read_stream(spark)
subscribers_restaurant_df = subscribers_restaurant_read(spark)
result_df = join_stream_subscribers(restaurant_read_stream_df, subscribers_restaurant_df)


result_df.writeStream\
    .foreachBatch(foreach_batch_function)\
        .start()\
            .awaitTermination()
