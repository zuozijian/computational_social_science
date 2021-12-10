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


# Combine the required data columns
def App_cat_get():
    # Import game and non-game categories separately
    pandas_df = pd.read_csv('apphl.csv', encoding='gbk')
    pandas_df.columns = ['x', 'y']
    values = pandas_df.values.tolist()
    columns = pandas_df.columns.tolist()
    df3 = spark.createDataFrame(values, columns)
    df3.registerTempTable('df3')
    detail_max = """create table app_cat
                    select * from df3"""
    spark.sql(detail_max)

    pandas_df = pd.read_csv('gamehl.csv', encoding='gbk')
    pandas_df.columns = ['x', 'y', 'z']
    values = pandas_df.values.tolist()
    columns = pandas_df.columns.tolist()
    df3 = spark.createDataFrame(values, columns)
    df3.registerTempTable('df3')
    detail_max = """create table game_cat
                    select * from df3"""
    spark.sql(detail_max)

    # Match the corresponding categories for 281 apps
    hql = """
    create table 281_APP_cat
    select distinct 
    brand_name as appname,
    package_name,
    app_cat2_name,
    y as hl
    from ad_message A
    join app_cat B
    on A.app_cat2_name = B.x
    and A.dayno = 20210601
    where B.y <> 'G'
    """
    spark.sql(detail_max)

    hql = """
    insert into 281_APP_cat
    select distinct 
    brand_name as appname,
    package_name,
    app_cat2_name,
    (case when z = '是' then 'GH' else 'GL' end) as hl
    from ad_message A
    join game_cat B
    on A.app_cat2_name = B.y
    and A.brand_name = B.x
    and A.dayno = 20210601
    """
    spark.sql(detail_max)

    # Count the usage time of each category app of the user
    hql = """
    create table 281_APP_cat_usetime
    select imei,
    hl,
    sum(duration) as sumtime
    from 281_APP_use A
    join 281_APP_cat B
    on A.appname = B.appname 
    group by imei,hl
    """
    spark.sql(detail_max)
    # Create a table for each category
    catlist = ['GH', 'GL', 'H', 'L']
    for i in range(4):
        hql = """
        create table 281_APP_""" + catlist[i] + """_usetime
        select distinct A.imei,B.hl,
        (case when B.sumtime is null then 0 else B.sumtime end) as sumtime
        from(select distinct imei from 
        281_APP_analysis_people) A left join
        281_APP_cat_usetime B
        on A.imei = B.imei 
        and hl = '""" + catlist[i] + """'
        """
        spark.sql(detail_max)
    # merge
    hql = """
    create table 281_APP_cat_usetime_conver
    select A.imei,
    A.sumtime as GH,
    B.sumtime as GL,
    C.sumtime as H,
    D.sumtime as L
    from
    281_APP_GH_usetime A join
    281_APP_GL_usetime B join
    281_APP_H_usetime C join
    281_APP_L_usetime D
    on A.imei = B.imei
    and A.imei = C.imei 
    and A.imei = D.imei 
    """
    spark.sql(detail_max)


def Conver_info_get():
    timelist = ['before', 'now', 'after']
    daynolist = [20210201, 20210301, 20210329, 20210426]
    # Insert APP search information
    for i in range(3):
        hql = """
        insert into table 281_APP_search
        select distinct
        A.imei,
        is_expose,
        appname,
        '""" + timelist[i] + """' as time,
        count(*) as searchnum
        from 281_APP_analysis_people A
        join browser_query_detail B
        on A.imei = B.imei
        and B.dayno >= """ + daynolist[i] + """
        and B.dayno <  """ + daynolist[i + 1] + """
        and query rlike appname
        group by 
        imei,
        is_expose,
        appname,
        time
        """
        spark.sql(hql)

    # Generate tables for each period
    Single_period_info_get()
    # Create a data table containing all conversion behaviors
    hql1 = """
    create table 281_APP_all_conver_message as
    select 
    A.imei,
    A.appname,
    A.is_expose,
    A.sum_data as nowuse,
    B.sum_data as beforeuse,
    C.sum_data as afteruse,
    D.sum_data as nowdown,
    E.sum_data as beforedown,
    F.sum_data as afterdown,
    G.sum_data as nowsearch,
    H.sum_data as beforesearch,
    I.sum_data as aftersearch,
    J.sum_data as isclick
    from 
    281_APP_use_before A join
    281_APP_use_now B join
    281_APP_use_after C join
    281_APP_install_before D join
    281_APP_install_now E join
    281_APP_install_after F join
    281_APP_search_before G join
    281_APP_search_now H join
    281_APP_search_after I join
    281_APP_click J
    on A.imei = B.imei
    and A.imei = C.imei 
    and A.imei = D.imei 
    and A.imei = E.imei 
    and A.imei = F.imei 
    and A.imei = G.imei 
    and A.imei = H.imei 
    and A.imei = I.imei 
    and A.imei = J.imei 
    and A.appname = B.appname 
    and A.appname = C.appname 
    and A.appname = D.appname 
    and A.appname = E.appname 
    and A.appname = F.appname 
    and A.appname = G.appname 
    and A.appname = H.appname 
    and A.appname = I.appname 
    and A.appname = J.appname 
    and A.is_expose = B.is_expose
    and A.is_expose = C.is_expose
    and A.is_expose = D.is_expose
    and A.is_expose = E.is_expose
    and A.is_expose = F.is_expose
    and A.is_expose = G.is_expose
    and A.is_expose = H.is_expose
    and A.is_expose = I.is_expose
    and A.is_expose = J.is_expose
    """
    spark.sql(hql)


def Single_period_info_get():
    # Divide all user information tables in each dimension into three tables according to the time of delivery
    timelist = ['before', 'now', 'after']
    converlist = ['281_APP_use', '281_APP_search']
    datalist = ['duration', 'searchnum']
    for j in range(2):
        for i in range(3):
            # Extract information for a certain period of time each time and create a separate table
            hql = """
            create table """ + converlist[j] + """_""" + timelist[i] + """
            select 
            A.imei,
            A.is_expose,
            A.appname,
            A.time,
            (case when """ + datalist[j] + """ is null then 0 else """ + datalist[j] + """ end) as sum_data
            from 
            (select
            * from
            281_APP_analysis_people 
            where time = """ + timelist[i] + """
            )A
            left join 
            (select
            * from
            """ + converlist[j] + """
            where time = """ + timelist[i] + """
            )B
            on A.imei = B.imei
            and A.appname = B.appname
            """
            spark.sql(hql)
    # Get whether to download attributes in each period
    for i in range(3):
        hql = """
        create table 281_APP_install_""" + timelist[i] + """
        select 
        A.imei,
        A.is_expose,
        A.appname,
        A.time,
        (case when B.imei is null then 0 else 1 end) as sum_data
        from 
        (select
        * from
        281_APP_analysis_people 
        where time = """ + timelist[i] + """
        )A
        left join 
        (select
        * from
        281_APP_install
        where time = """ + timelist[i] + """
        )B
        on A.imei = B.imei
        and A.appname = B.appname
        """
        spark.sql(hql)


def Portrait_message_get():
    # Divide portraits into multiple columns according to attributes
    genderlist = ['man', 'woman']
    agelist = ['_17', '18_24', '25_29', '30_34', '35_49', '_50']
    citylist = ['1', '2', '3', 'new1']
    pricelist = ['1000_2000', '2000_3000', '3000_4000', '4000_5000', '5000_6000', '_1000', '_6000']
    count = 0
    allhql = """
    create table people_portrait_conver as
    select imei,on_price1000,select on_price6000,base_age18_24,base_age25_29,base_age30_34,base_age35_49,base_age_17,base_age_50,base_gender_woman,base_gender_man,city_level_1,city_level_3,city_level_2,city_level_new1,on_price_1000_2000,on_price_2000_3000,on_price_3000_4000,on_price_4000_5000,on_price_5000_6000 from"""
    # Each attribute value is divided into a column
    for i in genderlist:
        count += 1
        hql = """
        (select imei,
        (case when base_gender is null then 0 else 1 end) as base_gender_""" + i + """
        (select
        imei
        from 
        281_APP_analysis_people 
        )A left join 
        (
        select * 
        from
        people_portrait
        where base_gender = '""" + i + """'
        )
        on A.imei = B.imei) s""" + str(count) + """ join
        """
        allhql += hql
    for i in agelist:
        count += 1
        hql = """
        (select imei,
        (case when base_gender is null then 0 else 1 end) as base_age_""" + i + """
        (select
        imei
        from 
        281_APP_analysis_people 
        )A left join 
        (
        select * 
        from
        people_portrait
        where base_age = '""" + i + """'
        )
        on A.imei = B.imei) s""" + str(count) + """ join
        """
        allhql += hql
    for i in citylist:
        count += 1
        hql = """
        (select imei,
        (case when base_gender is null then 0 else 1 end) as city_level_""" + i + """
        (select
        imei
        from 
        281_APP_analysis_people 
        )A left join 
        (
        select * 
        from
        people_portrait
        where city_level = '""" + i + """'
        )
        on A.imei = B.imei) s""" + str(count) + """ join
        """
        allhql += hql
    for i in pricelist:
        count += 1
        hql = """
        (select imei,
        (case when base_gender is null then 0 else 1 end) as on_price_""" + i + """
        (select
        imei
        from 
        281_APP_analysis_people 
        )A left join 
        (
        select * 
        from
        people_portrait
        where on_price = '""" + i + """'
        )
        on A.imei = B.imei) s""" + str(count) + """ 
        """
        allhql += hql
        if (count < 19):
            allhql += " join"
        else:
            allhql += " on"
    for i in range(17):
        allhql += ' s1.imei = s' + str(i + 2) + '.imei and '
    allhql += ' s1.imei = s19.imei'
    spark.sql(allhql)


def Merge_information():
    # people_portrait, 281_APP_all_conver_message.281_APP_cat_usetime_part
    # Use time to take log
    hql = """
    create table 281_APP_regression_data as
    select 
    A.imei,
    appname,
    is_expose,
    base_age18_24,base_age25_29,base_age30_34,base_age35_49,base_age_17,base_age_50,
    base_gender_woman,base_gender_man,
    city_level_1,city_level_3,city_level_2,city_level_new1,
    on_price_1000_2000,on_price_2000_3000,on_price_3000_4000,on_price_4000_5000,on_price_5000_6000,on_price1000,on_price6000,
    log10(nowuse+1) as nowuse,
    log10(beforeuse+1) as beforeuse,
    log10(afteruse+1) as afteruse,
    nowdown,
    beforedown,
    afterdown,
    log10(nowsearch+1) as nowsearch,
    log10(beforesearch+1) as beforesearch,
    log10(aftersearch+1) as aftersearch,
    isclick,
    log10(GH+1) as GH,
    log10(GL+1) as GL,
    log10(H+1) as H,
    log10(L+1) as L
    from
    people_portrait_conver A join 
    281_APP_all_conver_message B join
    281_APP_cat_usetime_conver C
    on  A.imei = B.imei
    and B.imei = C.imei
    """
    spark.sql(hql)


def main():
    # I Get the four categories of APP
    App_cat_get()
    # II Get APP conversion information
    Conver_info_get()
    # III Get crowd portrait information
    Portrait_message_get()
    # IV Combine three categories of information to obtain regression data table
    Merge_information()