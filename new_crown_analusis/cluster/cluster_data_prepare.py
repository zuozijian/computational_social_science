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


def get_clus_user_mes():
    # Extract 10,000 imei information and make statistics according to the new classification
    hql = """
    create table $city_part_active_people_use as
    select 
    B.*
    (
    select
    distinct imei
    from
    $city_active_people_use
    limit 10000
    )A join
    $city_active_people_use B
    on A.imei = B.imei
    """
    spark.sql(hql)

    hql = """
    create table $city_part_active_people_newcatuse as
    select
    A.imei,
    dayno,
    newcat,
    sum(start_duration) as sumtime
    from
    $city_part_active_people_use A
    join
    app_new_cat B
    on A.appname = B.Appname
    group by
    A.imei,
    dayno,
    newcat
    """
    spark.sql(hql)

    # Divided into 36 dimensions according to the new classification
    hql = """
    create table $city_lda_data as
    select 
    imei,
    dayno
    """
    # Get each category to reconstruct the data table by column
    newcatlist = get_newcat()
    count = 1
    for i in newcatlist:
        hql += """,sum(case when newcat = '""" + i + """' then sumtime else 0) as cat""" + str(count)
        count += 1
    hql += """
    from 
    $city_part_active_people_newcatuse
    group by
    imei,
    dayno
    """
    spark.sql(hql)

    # divide each column by the mean and perform log processing
    hql = """
    create table $city_lda_data_mean as
    select 
    imei,
    """
    count = 1
    for i in newcatlist:
        hql += """mean(cat""" + str(count) + ') as cat' + str(count) + ','
        count += 1
    hql = hql.strip(',')
    hql += """
    from 
    $city_part_active_people_newcatuse
    group by 
    imei
    """
    spark.sql(hql)

    # log
    hql = """
    create table $city_lda_data_log as
    select 
    imei,
    dayno
    """
    count = 1
    for i in newcatlist:
        hql += """,(log10(A.cat""" + str(count) + ')+1)/(log10(B.cat' + str(count) + ')+1) as cat' + str(count)
        count += 1
    hql += """
    from 
    $city_lda_data_mean A
    join
    $city_lda_data B
    on A.imei = B.imei
    """
    spark.sql(hql)

    # Combine the imei number with the date
    # When there are multiple cities, you need to renumber them and insert them into the table
    hql = """
    create table $city_lda_data_log_daynum as
    select 
    concat(imei_number,'-',dayno) daynum
    """
    count = 1
    for i in newcatlist:
        hql += ',cat' + str(count)
        count += 1
    hql += """
    from
    (select 
    *,
    row_number() over(partition by dayno order by dayno) as imei_number,
    from 
    $city_lda_data_log)A
    """
    spark.sql(hql)


def get_newcat():
    # Get the name of each category
    hql = """
    select 
    distinct newcat
    from  app_new_cat 
    """
    df = spark.sql(hql)
    appdf = df.toPandas()
    appline = appdf['newcat'].unique()
    return appline


def main():
    # Take a sample of the population, sort them out according to classification, and finally take the log through the mean
    get_clus_user_mes()