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


def app_use_sum_get():
    # Get the usage of each APP
    hql = """
    create table $city_active_people_use_packsum as
    select 
    city,
    pack_name,
    dayno,
    sum(start_duration) as sumtimes,
    count(distinct imei) as sumimei
    from  $city_active_people_use
    group by 
    city,
    pack_name,
    dayno
    """
    maxdf = spark.sql(hql)

    # Statistics by appname
    hql = """
    create table $city_active_people_use_appsum as
    select 
    city,
    app_name,
    dayno,
    sum(sumtimes) as sumtimes,
    max(sumimei) as sumimei
    from 
    (select app_name
    ,package_name
    from ad_message
    where dayno = 20211101
    group by 
    app_name
    ,package_name
    )A
    join $city_active_people_use_packsum B
    on A.package_name = B.pack_name
    group by 
    city,
    app_name,
    dayno
    """
    maxdf = spark.sql(hql)


def poi_mes_get():
    # Get all poi records of active users during the time period
    hql = """
    create table active_poi_mes as
    select
    A.imei,
    dayno,
    city,
    base_gender,
    base_age,
    poi_name,
    level_1,
    level_2,
    level_3,
    count(*) as visitnum
    from $city_active_people_use A
    join people_portrait B
    join poi_message C
    on A.imei = B.imei
    and B.imei = C.imei
    and dayno >= $time_begin
    and dayno  < $time_end
    group by 
    A.imei,
    dayno,
    city,
    base_gender,
    base_age,
    poi_name,
    poi_name,
    level_1,
    level_2,
    level_3
    """
    # Count weekly poi records according to the time range
    day1 = dayno_add_day("20201101", "20210331", 7)
    day2 = dayno_add_day("20201107", "20210331", 7)
    for i in range(len(day1)):
        hql = """
        insert into active_poi_mes
        select * from(
        select
        dayno,
        city,
        base_gender,
        base_age,
        poi_name,
        level_1,
        level_2,
        level_3,
        visitnum,
        imeinum,
        row_number() over(partition by dayno order by visitnum desc) as imei_number
        from
        (
        select 
        '""" + str(day1[i]) + '~' + str(day2[i]) + """' as dayno,
        city ,
        base_gender,
        base_age,
        poi_name,
        level_1,
        level_2,
        level_3,
        sum(visitnum) as visitnum,
        count(distinct imei) as imeinum
        from active_poi_mes
        where dayno >= """ + str(day1[i]) + """
        and dayno <= """ + str(day2[i]) + """
        group by dayno,city ,
        base_gender,
        base_age,
        poi_name,
        level_1,
        level_2,
        level_3)
        )
        """
        spark.sql(hql)


def main():
    # Get app usage status of active users
    app_use_sum_get()
    # Get the weekly poi distribution during the active user period
    poi_mes_get()