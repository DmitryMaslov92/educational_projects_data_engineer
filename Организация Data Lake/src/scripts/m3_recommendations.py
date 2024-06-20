import sys
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window




def main():

        date = sys.argv[1]
        base_input_path = sys.argv[2]
        city_geo_path = sys.argv[3]
        actual_cities_path = sys.argv[4]
        base_output_path = sys.argv[5]

        conf = SparkConf().setAppName(f"Recommendations-{date}").config("spark.sql.autoBroadcastJoinThreshold", "-1")
        sc = SparkContext(conf=conf)
        spark = SQLContext(sc)

        events = spark.read.parquet(f'{base_input_path}').where(f'date < {date}')

        df = events \
                .where("event_type=='message' and 'lat' is not null and 'lon' is not null")\
                .withColumn('dt', F.coalesce('event.message_ts', 'event.datetime'))
        
        geo_df = spark.read \
                .options(delimiter=';', header=True) \
                .csv(f'{city_geo_path}') \
                .withColumn('lat', (F.regexp_replace(F.col('lat'), ',', '.').cast("double"))) \
                .withColumn('lng', (F.regexp_replace(F.col('lng'), ',', '.').cast("double")))
        
        act_city = spark.read.parquet(f'{actual_cities_path}/date={date}') 

        act_city = act_city \
                .join(geo_df, act_city.actual_city==geo_df.city, 'left') \
                .select(F.col('user_id'), 
                        F.col('id').alias('zone_id'), 
                        F.col('city'), 
                        F.col('timezone'), 
                        F.col('local_time'),
                        )
        
        user_last_loc = df\
                        .withColumn('rank', F.row_number().over(Window().partitionBy("event.message_from").orderBy(F.col("dt").asc()))) \
                        .where("rank == 1")\
                        .select(F.col('event.message_from').alias('user_id'), F.col('lat'), F.col('lon'))
        
        user_contacts = df.where('event.message_to is not null')\
                        .selectExpr("event.message_from as user_left", "event.message_to as user_right") \
                        .unionByName(
                                df.where('event.message_to is not null')\
                                .selectExpr("event.message_to as user_left", "event.message_from as user_right")
                        )\
                        .filter(F.col('user_left')!=F.col('user_right'))\
                        .distinct()
        
        subscriptions = events \
                        .filter(F.col("event_type") == "subscription") \
                        .where("event.subscription_channel is not null") \
                        .selectExpr("event.user as user_id", "event.subscription_channel as channel") \
                        .distinct()
        

        all_pairs = df.select(F.col('event.message_from').alias('user_left')).distinct() \
                        .crossJoin(df.select(F.col('event.message_from').alias('user_right')).distinct()) \
                        .filter(F.col('user_left')!=F.col('user_right'))
        
        no_contact = all_pairs.join(user_contacts, 'user_left', 'leftanti').persist()

        with_channels = no_contact.join(
                                        subscriptions.selectExpr('user_id as user_left', 'channel as channels_left'),  'user_left', 'left'
                                ).join(
                                        subscriptions.selectExpr('user_id as user_right', 'channel as channels_right'),  'user_right', 'left'
                                ).filter(F.col('channels_left')==F.col('channels_right'))

        with_loc = with_channels.join(
                                        user_last_loc.selectExpr('user_id as user_left', 'lat as lat_left', 'lon as lon_left'),  'user_left', 'left'
                                ).join(
                                        user_last_loc.selectExpr('user_id as user_right', 'lat as lat_right', 'lon as lon_right'),  'user_right', 'left'
                                ).withColumn('distance', F.lit(2*6371) * F.asin(
                                                F.sqrt(
                                                F.pow(F.sin((F.radians(F.col('lat_left')) - F.radians(F.col('lat_right'))) / F.lit(2)), 2) + \
                                                F.cos(F.radians(F.col('lat_left'))) * F.cos(F.radians(F.col('lat_right'))) * \
                                                F.pow(F.sin((F.radians(F.col('lon_left')) - F.radians(F.col('lon_right'))) / F.lit(2)), 2)
                                                )))\
                                .where(F.col('distance') <= 1).persist()
        


        with_loc.join(act_city, act_city.user_id == with_loc.user_left)\
                .withColumn('processed_dttm', F.current_timestamp())\
                .withColumn('local_time', F.from_utc_timestamp(F.col('processed_dttm'), F.col('timezone')))\
                .select('user_left', 'user_right', 'processed_dttm', 'zone_id','local_time')\
                .write.parquet(f'{base_output_path}/date={date}')
        
        no_contact.unpersist()
        with_loc.unpersits()


if __name__ == "__main__":
        main()


