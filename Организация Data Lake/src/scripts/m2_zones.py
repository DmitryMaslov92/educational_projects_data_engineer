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

        conf = SparkConf().setAppName(f"ZonesMart-{date}")
        sc = SparkContext(conf=conf)
        spark = SQLContext(sc)

        df = spark.read.parquet(f'{base_input_path}') \
                .where(f'date < {date}') \
                .withColumn('dt', F.coalesce('event.message_ts', 'event.datetime')) \
                .withColumn('user_id', F.coalesce('event.user', 'event.message_from', 'event.reaction_from')) \
                .select('user_id', 'date', 'dt', 'lat', 'lon', 'event_type')
        
        registrations = df \
                .withColumn("rank", F.row_number().over(
                    Window().partitionBy("user_id").orderBy(F.col("dt").asc()))) \
                .filter(F.col("rank") == 1) \
                .drop('rank') \
                .withColumn("event_type", F.lit("registration"))
        
        geo_df = spark.read \
            .options(delimiter=';', header=True) \
            .csv(f'{city_geo_path}') \
            .select(F.col('id'), F.col('city').alias('actual_city'))
        
        act_city = spark.read.parquet(f'{actual_cities_path}/date={date}') \
                        .join(geo_df, 'actual_city', 'left') \
                        .select(F.col('user_id'), F.col('id').alias('zone_id'))
    
        
        df_with_cities = df.unionByName(registrations)\
                            .join(act_city, 'user_id', 'inner')\
                            .withColumn("month", F.trunc(F.col("date"), 'month')) \
                            .withColumn("week",  F.trunc(F.col("date"), 'week'))
        
        df_monthly = df_with_cities.groupBy("zone_id", "month") \
                                    .pivot("event_type", ['message', 'reaction', 'subscription', 'registration']) \
                                    .count() \
                                    .withColumnRenamed('message', 'month_message') \
                                    .withColumnRenamed('reaction', 'month_reaction') \
                                    .withColumnRenamed('subscription', 'month_subscription') \
                                    .withColumnRenamed('registration', 'month_user')
        
        df_weekly = df_with_cities.groupBy("zone_id", "month") \
                                    .pivot("event_type", ['message', 'reaction', 'subscription', 'registration']) \
                                    .count() \
                                    .withColumnRenamed('message', 'week_message') \
                                    .withColumnRenamed('reaction', 'week_reaction') \
                                    .withColumnRenamed('subscription', 'week_subscription') \
                                    .withColumnRenamed('registration', 'week_user')

        df_monthly.join(df_weekly, ["zone_id"], "full").na.fill(0).write.parquet(f'{base_output_path}/date={date}')



if __name__ == "__main__":
        main()


