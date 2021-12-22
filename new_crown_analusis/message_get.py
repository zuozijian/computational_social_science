import datetime
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import IntegerType, DoubleType, StringType, ArrayType, MapType


def get_spark_sess(master="local[*]", deploy_mode='client'):
    spark_sess = SparkSession.builder.master(master).config(
        "spark.submit.deployMode", deploy_mode).enableHiveSupport().getOrCreate()
    spark_sess.sparkContext.setLogLevel("OFF")
    print("get spark session done.")
    return spark_sess


spark = get_spark_sess()


def dayno_add_day(start_dayno, end_dayno, add_day=1):
    # Generate a date range by week
    res = []
    res.append(int(start_dayno))
    date_string = str(start_dayno)
    # begin_date
    dt = datetime.datetime.strptime(date_string[:4] + '-' + date_string[4:6] + '-' + date_string[6:8], '%Y-%m-%d')
    # end_date
    end_dayno = datetime.datetime.strptime(end_dayno[:4] + '-' + end_dayno[4:6] + '-' + end_dayno[6:8], '%Y-%m-%d')
    out_dayno = datetime.datetime.strptime((dt + datetime.timedelta(days=add_day)).strftime("%Y-%m-%d"), "%Y-%m-%d")
    while (dt < out_dayno and out_dayno <= end_dayno):
        result = int(out_dayno.strftime('%Y-%m-%d').replace('-', ''))
        res.append(result)
        dt = out_dayno
        out_dayno = datetime.datetime.strptime((dt + datetime.timedelta(days=add_day)).strftime("%Y-%m-%d"), "%Y-%m-%d")
    return res


def City_message_get():
    # Crowd portrait
    hql = """
    create table people_portrait as 
    select imei,
    (case when city is null then 'nan' else city end)as city,
    (case 
    when base_gender = 1 then 'man'
    when base_gender = 0 then 'woman'
    else 'nan' end)as base_gender,
    (case 
    when base_age = 0 then '_17'
    when base_age = 1 then '18_24'
    when base_age = 2 then '25_29'
    when base_age = 3 then '30_34'
    when base_age = 4 then '35_49'
    when base_age = 5 then '_50'
    else 'nan' end)as base_age,
    (case 
    when city_level = '1' then '1' 
    when city_level = 'new1' then 'new1' 
    when city_level = '2' then '2'
    when city_level = '3' then '3'
    else 'nan' end )as city_level,
    (case 
    when cast(on_price as int) <= 1000 then '_1000' 
    when cast(on_price as int) > 1000 and cast(on_price as int) <= 2000 then '1000_2000' 
    when cast(on_price as int) > 2000 and cast(on_price as int) <= 3000 then '2000_3000' 
    when cast(on_price as int) > 3000 and cast(on_price as int) <= 4000 then '3000_4000' 
    when cast(on_price as int) > 4000 and cast(on_price as int) <= 5000 then '4000_5000' 
    when cast(on_price as int) > 5000 and cast(on_price as int) <= 6000 then '5000_6000' 
    else '_6000' end )as on_price
    from people_message A
    inner join city_level  B
    on A.model = B.model
    and dayno = 20211101
    """
    spark.sql(hql)

    # Create a table of urban crowds
    hql = """
    create table $city_all_people as
    select 
    imei,
    city
    from people_portrait
    where city = '$city' 
    """
    maxdf = spark.sql(hql)

    # Get crowd usage information
    hql = """
    create table $city_all_people_use as
    select 
    A.imei,
    city,
    pack_name,
    start_times,
    start_duration,
    dayno
    from $city_all_people A
    join app_use B
    on A.imei = B.imei 
    and dayno >= $time_begin
    and dayno  < $time_end
    """
    maxdf = spark.sql(hql)

    # Get a list of the start and end dates of each week
    day1 = dayno_add_day("20210301", "20210829", 7)
    day2 = dayno_add_day("20210307", "20210831", 7)

    # Get the weekly usage time table
    for i in range(1, len(day1)):
        print(day1[i])
        hql = """
        insert into $city_all_people_use_sum
        select 
        imei,
        sum(start_duration) as sumtime
        from
        $city_all_people_use
        where dayno >= """ + str(day1[i]) + """
        and dayno <=  """ + str(day2[i]) + """
        group by imei
        having sumtime > 25200
        """
        maxdf = spark.sql(hql)

    # Get active users
    hql = """
    create table $city_active_people as
    select 
    imei,
    count(*) as imeinums
    from
    $city_all_people_use_sum
    group by imei
    having imeinums = """ + str(len(day1)) + """
    """
    maxdf = spark.sql(hql)

    # Get usage information of active users
    hql = """
    create table $city_active_people_use as
    select 
    B.*
    from
    $city_active_people A
    join $city_all_people_use B
    on A.imei = B.imei
    """
    maxdf = spark.sql(hql)


def main():
    # Obtain the information of active users in the required city according to the time range
    City_message_get()