import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf ,explode
from pyspark.sql.types import IntegerType,DoubleType,StringType,ArrayType,MapType


def get_spark_sess(master="local[*]", deploy_mode='client'):
    spark_sess = SparkSession.builder.master(master).config(
        "spark.submit.deployMode", deploy_mode).enableHiveSupport().getOrCreate()
    spark_sess.sparkContext.setLogLevel("OFF")
    print("get spark session done.")
    return spark_sess
spark = get_spark_sess()

def ad_get():
    # Search for the advertising information that has been exposed during the time period of the search
    hql = """
    create table APP_message_m3 as
    select brand_name,app_cat2_name,
    sum(expose_cnt) as expose_cnt
    ,count(distinct A.ad_id) as adnum
    from  ad_message A
    inner join ad_expose_message B
    on A.ad_id = B.ad_id
    and B.dayno = 20210503
    and A.dayno >=20210301
    and A.dayno <=20210328
    group by brand_name,app_cat2_name
    """
    spark.sql(hql)

    # Select advertisements with more than 10,000 impressions, and get the number of advertisements 281
    hql = """
    create table 281_APP as
    select brand_name
    from  APP_message_m3 A
    where expose_cnt >= 10000
    """
    spark.sql(hql)

    # Obtained 281 app ads exposed users during this period
    hql = """
    create table 281_APP_expose_people as
    select distinct imei,
    C.brand_name as appname
    from  ad_expose_message A
    inner join ad_message B
    inner join 281_APP C
    on A.ad_id = B.ad_id
    and B.brand_name = C.brand_name
    and B.app_cat2_name = C.app_cat2_name
    and B.dayno = 20210503
    and A.dayno >=20210201
    and A.dayno <=20210425
    """
    spark.sql(hql)


def analysis_people_get():
    # 1. Get exposed users with usage behaviors
    hql = """
    create table 281_APP_analysis_people as
    select
    A.imei,
    A.appname,
    '1' as is_expose
    from 281_APP_expose_people A
    join app_use B
    on A.ad_id = B.ad_id
    and B.dayno >=20210201
    and B.dayno <=20210425
    """
    spark.sql(hql)
    # 2. Get the same amount of exposed users who have no behavior
    # Get the number of people per app
    hql = "select appname,count(*) as imei_nums from 281_APP_analysis_people group by appname"
    df = spark.sql(hql)
    df2 = df.toPandas()
    # Dictionary storage
    imeinumdict = dict(zip(df2['brand_name'], df2['num']))
    # Insert APP by APP
    for appname in list(imeinumdict.keys()):
        # Get the inserted APP name and number of people
        app_name = appname
        imei_nums = imeinumdict[app_name]
        # Use left association when search does not exist
        hql = """
        insert into 281_APP_analysis_people
        select distinct A.imei
        ,'""" + app_name + """' as appname
        ,1 as is_expose
        from
        281_APP_expose_people A
        LEFT JOIN
        281_APP_analysis_people B
        on A.imei = B.imei
        and B.appname = A.appname
        and B.appname = '""" + app_name + """'
        and B.imei is null
        limit """ + str(imei_nums)
        spark.sql(hql)
    # 3. Get the same amount of non-exposed users
    # Steps are similar to 2. At this time, users need to be filtered from the total user table
    hql = "select appname,count(*) as imei_nums from 281_APP_analysis_people group by appname"
    df = spark.sql(hql)
    df2 = df.toPandas()
    # Dictionary storage
    imeinumdict = dict(zip(df2['brand_name'], df2['num']))
    # Insert APP by APP
    for appname in list(imeinumdict.keys()):
        # Get the inserted APP name and number of people
        app_name = appname
        imei_nums = imeinumdict[app_name]
        # Use left association when search does not exist
        hql = """
        insert into 281_APP_analysis_people
        select distinct A.imei
        ,'""" + app_name + """' as appname
        ,0 as is_expose
        from
        browser_browse A
        LEFT JOIN
        281_APP_analysis_people B
        on A.imei = B.imei
        and B.appname = '""" + app_name + """'
        and B.imei is null
        limit """ + str(imei_nums)
        spark.sql(hql)


def conver_message_get():
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

    # Installation Information
    # Before advertising
    hql = """
    create table 281_APP_install as 
    select distinct
    A.imei,
    is_expose,
    appname,
    'before' as time
    from 281_APP_analysis_people A
    join app_install B
    on A.appname = B.appname
    and A.imei = B.imei
    and B.dayno >= 20210201
    and B.dayno <  20210301
    and is_upgrade_install = 0
    """
    spark.sql(hql)
    # advertising
    hql = """
    insert into table 281_APP_install
    select distinct
    A.imei,
    is_expose,
    appname,
    'now' as time
    from 281_APP_analysis_people A
    join app_install B
    on A.appname = B.appname
    and A.imei = B.imei
    and B.dayno >= 20210301
    and B.dayno <  20210329
    and is_upgrade_install = 0
    """
    spark.sql(hql)
    # after advertising
    hql = """
    insert into table 281_APP_install
    select distinct
    A.imei,
    is_expose,
    appname,
    'after' as time
    from 281_APP_analysis_people A
    join app_install B
    on A.appname = B.appname
    and A.imei = B.imei
    and B.dayno >= 20210329
    and B.dayno <  20210426
    and is_upgrade_install = 0
    """
    spark.sql(hql)

    # Use Information
    # Before advertising
    hql = """
    create table 281_APP_use as 
    select 
    A.imei,
    is_expose,
    appname,
    'before' as time,
    sum(duration) as duration
    from 281_APP_analysis_people A
    join app_us B
    on A.appname = B.appname
    and A.imei = B.imei
    and B.dayno >= 20210201
    and B.dayno <  20210301
    group by 
    imei,
    is_expose,
    appname,
    time
    """
    spark.sql(hql)
    # advertising
    hql = """
    insert into table 281_APP_use
    select distinct
    A.imei,
    is_expose,
    appname,
    'now' as time,
    sum(duration) as duration
    from 281_APP_analysis_people A
    join app_use B
    on A.appname = B.appname
    and A.imei = B.imei
    and B.dayno >= 20210301
    and B.dayno <  20210329
    group by 
    imei,
    is_expose,
    appname,
    time
    """
    spark.sql(hql)
    # after advertising
    hql = """
    insert into table 281_APP_use
    select distinct
    A.imei,
    is_expose,
    appname,
    'after' as time,
    sum(duration) as duration
    from 281_APP_analysis_people A
    join app_use B
    on A.appname = B.appname
    and A.imei = B.imei
    and B.dayno >= 20210329
    and B.dayno <  20210426
    group by 
    imei,
    is_expose,
    appname,
    time
    """
    spark.sql(hql)

    # Pay Information
    # Before advertising
    hql = """
    create table 281_APP_pay as 
    select distinct
    A.imei,
    is_expose,
    appname,
    'before' as time
    from 281_APP_analysis_people A
    join app_pay B
    on A.appname = B.appname
    and A.imei = B.imei
    and B.dayno =  20210301
    and B.recent_pay_dayno >= 20210201
    """
    spark.sql(hql)
    # advertising
    hql = """
    insert into table 281_APP_pay
    select distinct
    A.imei,
    is_expose,
    appname,
    'now' as time
    from 281_APP_analysis_people A
    join app_pay B
    on A.appname = B.appname
    and A.imei = B.imei
    and B.dayno =  20210329
    and B.recent_pay_dayno >= 20210301
    """
    spark.sql(hql)
    # after advertising
    hql = """
    insert into table 281_APP_pay
    select distinct
    A.imei,
    is_expose,
    appname,
    'after' as time
    from 281_APP_analysis_people A
    join app_pay B
    on A.appname = B.appname
    and A.imei = B.imei
    and B.dayno =  20210426
    and B.recent_pay_dayno >= 20210329
    """
    spark.sql(hql)

    # Ad click
    line = get_appname()
    for appname in line:
        hql = """
        insert into 281_APP_click
        select 
        A.imei,
        A.appname,
        A.is_expose,
        (case when B.imei is null then 0 else 1 end) as is_click
        from (
            select * from 281_APP_analysis_people A
            where A.appname = '""" + appname + """') A
        left join ad_expose B
        on A.imei = B.imei
        and B.dayno >= 20210301
        and B.dayno <  20210329
        and A.appname = B.appname 
        and click_nums > 0
        """
        spark.sql(hql)


def get_appname():
    # Get the names of 281 apps
    hql = """
    select 
    distinct appname
    from  281_APP_use 
    """
    df = spark.sql(hql)
    appdf = df.toPandas()
    appline = appdf['appname'].unique()
    return appline


def main():
    # I Get the ads that need to be analyzed
    ad_get()
    # II Get the analysis population of each APP
    analysis_people_get()
    # III Get the conversion behavior of each APP user
    conver_message_get()