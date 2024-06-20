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


def find_home_city(cities):

    # функция для расчета домашнего города
    # домашним будет считаться город, из которого юзер писал непрерывно в течение 27 дней
    # для новых юзеров, домашним будем считать город, из которого писалось больше всего сообщений
    # либо, из которого было написано первое сообщение при прочих равных
    prev_city = cities[0]
    first_city = cities[-1]
    count = 0
    max_count = 0
    city_with_max_count = ""
    for cur_city in cities:
        if count >= max_count:
            max_count = count
            city_with_max_count = prev_city
            
        if prev_city == cur_city:
            count += 1
        else:
            count = 0
            
        prev_city = cur_city
        
        if count == 27:
            return cur_city
        if max_count == 1:
             return first_city
    return city_with_max_count


def count_travels(cities):
    prev_city = ''
    count = 0
    for cur_city in cities:
        if prev_city != cur_city:
            count = count + 1
        
        prev_city = cur_city
    
    return count


def find_travels(cities):
    prev_city = ''
    travel_list = []
    for cur_city in cities:
        if prev_city != cur_city:
            travel_list.append(cur_city)
            
        prev_city = cur_city
            
    return travel_list


def main():

        date = sys.argv[1]
        base_input_path = sys.argv[2]
        city_geo_path = sys.argv[3]
        base_output_path = sys.argv[4]

        find_home_city_udf = F.udf(lambda x:find_home_city(x))
        count_travels_udf = F.udf(lambda x:count_travels(x))
        find_travels_udf = F.udf(lambda x:find_travels(x))

        conf = SparkConf().setAppName(f"UserGeo-{date}")
        sc = SparkContext(conf=conf)
        spark = SQLContext(sc)

        aus_timezones = [
            'ACT',
            'Adelaide',
            'Brisbane',
            'Broken_Hill',
            'Canberra',
            'Currie',
            'Darwin',
            'Eucla',
            'Hobart',
            'LHI',
            'Lindeman',
            'Lord_Howe',
            'Melbourne',
            'NSW',
            'North',
            'Perth',
            'Queensland',
            'South',
            'Sydney',
            'Tasmania',
            'Victoria',
            'West',
            'Yancowinna',
        ]

        df = spark.read.parquet(f'{base_input_path}').where(f"event_type=='message' and date < {date}") \
            .withColumn('dt', F.coalesce('event.message_ts', 'event.datetime'))

        window = Window().partitionBy("event.message_id").orderBy(F.col("distance").asc())

        geo_df = spark.read \
            .options(delimiter=';', header=True) \
            .csv(f'{city_geo_path}') \
            .withColumn('lat', (F.regexp_replace(F.col('lat'), ',', '.').cast("double"))) \
            .withColumn('lng', (F.regexp_replace(F.col('lng'), ',', '.').cast("double")))

        sub_df = df.where("lat is not null") \
            .crossJoin(geo_df.selectExpr(['city', 'lat as city_lat', 'lng as city_lng'])) \
            .withColumn('distance', F.lit(2*6371) * F.asin(
                F.sqrt(
                    F.pow(F.sin((F.radians(F.col('city_lat')) - F.radians(F.col('lat'))) / F.lit(2)), 2) + \
                    F.cos(F.radians(F.col('city_lat'))) * F.cos(F.radians(F.col('lat'))) * \
                    F.pow(F.sin((F.radians(F.col('city_lng')) - F.radians(F.col('lon'))) / F.lit(2)), 2)
                ))) \
            .withColumn('rank', F.row_number().over(window)) \
            .where("rank == 1")
            
        
        # т.к. не все города из списка есть в списке таймзон, добавим еще расчет ближайшего центра таймзоны

        sub_df = sub_df\
            .select(F.col('event'), F.col('lat'), F.col('lon'),F.col('date'), F.col('dt'),F.col('city'))\
            .crossJoin(geo_df[geo_df.city.isin(aus_timezones)].selectExpr(['city as tz_city', 'lat as city_lat', 'lng as city_lng'])) \
            .withColumn('distance', F.lit(2*6371) * F.asin(
                F.sqrt(
                    F.pow(F.sin((F.radians(F.col('city_lat')) - F.radians(F.col('lat'))) / F.lit(2)), 2) + \
                    F.cos(F.radians(F.col('city_lat'))) * F.cos(F.radians(F.col('lat'))) * \
                    F.pow(F.sin((F.radians(F.col('city_lng')) - F.radians(F.col('lon'))) / F.lit(2)), 2)
                ))) \
            .withColumn('tz_rank', F.row_number().over(window)) \
            .where("tz_rank == 1")


        sub_df.persist()

        # найдем актуальный город
        act_city = sub_df \
            .select(F.col('event.message_from').alias('user_id'), F.col('dt'), F.col('date'), F.col('city')) \
            .withColumn('rank', F.row_number().over(Window().partitionBy("user_id").orderBy(F.col("date").desc(), F.col('dt').desc()))) \
            .where('rank==1') \
            .select(F.col('user_id'), F.col('city').alias('actual_city'))

        # найдем домашний город, количество поездок и список городов посещения
        mid_df = act_city.join(sub_df \
            .select(F.col('event.message_from').alias('user_id'), F.col('date'), F.col('city')) \
            .distinct() \
            .orderBy(F.col("user_id"), F.col("date").desc()) \
            .groupBy("user_id") \
            .agg(F.collect_list("city")) \
            .withColumn("home_city", F.lit(find_home_city_udf("collect_list(city)"))) \
            .withColumn("travels_count", F.lit(count_travels_udf("collect_list(city)"))) \
            .withColumn("travel_array", F.lit(find_travels_udf("collect_list(city)"))) \
            .orderBy(F.col("user_id")), 'user_id', 'outer')\
            .select(F.col('user_id'), F.col('actual_city'), F.col('home_city'), F.col('travels_count'), F.col('travel_array'))

        mid_df.persist()

        # найдем местное время и таймзону
        timezones = sub_df \
            .select(F.col('event.message_from').alias('user_id'), F.col('dt').cast('timestamp').alias("TIME_UTC"), F.col('date'), F.col('tz_city')) \
            .withColumn('rank', F.row_number().over(Window().partitionBy("user_id").orderBy(F.col("date").desc(), F.col('TIME_UTC').desc()))) \
            .where('rank==1') \
            .withColumn('timezone', F.concat(F.lit('Australia/'), F.col('tz_city')))\
            .withColumn('local_time', F.from_utc_timestamp(F.col('TIME_UTC'), F.col('timezone'))) \
            .select(F.col('user_id'), F.col('timezone'), F.col('local_time'))

        mid_df.join(timezones, 'user_id', 'outer').write.mode("overwrite").parquet(f'{base_output_path}/date={date}')

        mid_df.unpersist()
        sub_df.unpersist()

if __name__ == "__main__":
        main()


