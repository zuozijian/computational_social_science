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


def stratifying_did(appname, list):
    # Obtain statistical population stratification information
    hql = """
    select 
    base_age,
    base_gender,
    city_level,
    on_price,
    is_exposed,
    count(distinct B.imei)
    from people_portrait A
    inner join 281_APP_use B
    on A.imei = B.imei
    and B.appname = '""" + appname + """'
    group by 
    base_age,
    base_gender,
    city_level,
    on_price,
    is_exposed
    """
    df = spark.sql(hql)
    list = get_floor_num(df, list, appname)
    return list


def get_floor_num(df, list, appname):
    # Get the number of exposed and unexposed layers
    df2 = df.toPandas()
    df2_expose1 = df2[df2['is_exposed'] == 1]
    list.append(df2_expose1.iloc[:, 0].size)
    df2_expose0 = df2[df2['is_exposed'] == 0]
    list.append(df2_expose0.iloc[:, 0].size)
    list = get_use_floor_num(df2_expose1, df2_expose0, list, appname)
    return list


def get_use_floor_num(df2_expose1, df2_expose0, list, appname):
    # Number of layers used for combined exposure and non-exposure output
    df_merge = pd.merge(df2_expose1, df2_expose0, how='inner', on=['base_age', 'base_gender', 'city_level', 'on_price'])
    list.append(df_merge.iloc[:, 0].size)
    # print('use_num',df_merge.iloc[:,0].size)
    # Modify column name
    df_merge.columns = ['base_age', 'base_gender', 'city_level', 'on_price', 'is_exposed1', 'count1', 'is_exposed0',
                        'count0']
    df_merge
    # Extract and merge exposure and non-exposure information separately
    df_merge1 = df_merge[['base_age', 'base_gender', 'city_level', 'on_price', 'is_exposed1', 'count1']]
    df_merge0 = df_merge[['base_age', 'base_gender', 'city_level', 'on_price', 'is_exposed0', 'count0']]
    # Calculate the number of users
    list.append(df_merge1['count1'].sum())
    list.append(df_merge0['count0'].sum())
    # Modify column name
    df_merge1.columns = ['base_age', 'base_gender', 'city_level', 'on_price', 'is_exposed', 'count']
    df_merge0.columns = ['base_age', 'base_gender', 'city_level', 'on_price', 'is_exposed', 'count']
    # merge
    df_allpeople = pd.merge(df_merge1, df_merge0, how='outer')
    # Sorting
    df_allpeople = df_allpeople.sort_values(['base_age', 'base_gender', 'city_level', 'on_price', 'is_exposed'])
    list = get_did_num(df_allpeople, df_merge1, df_merge0, list, appname)
    return list


def get_did_num(df_allpeople, df_merge1, df_merge0, list, appname):
    # Get the dataframe of the three launch time periods
    df_three = three_time_message(appname)
    # Filtering extracts three time periods: before advertising, during advertising, and after advertising
    df_before = df_three[df_three['time'] == 'before']
    df_now = df_three[df_three['time'] == 'now']
    df_after = df_three[df_three['time'] == 'after']
    # Separately count the number of people before, during and after the launch
    df_beforep = df_before.toPandas()
    df_nowp = df_now.toPandas()
    df_afterp = df_after.toPandas()
    # Outreach to the main table
    df_allpeople_b = pd.merge(df_allpeople, df_beforep, how='left',
                              on=['base_age', 'base_gender', 'city_level', 'on_price', 'is_exposed']).fillna(0)
    df_allpeople_n = pd.merge(df_allpeople, df_nowp, how='left',
                              on=['base_age', 'base_gender', 'city_level', 'on_price', 'is_exposed']).fillna(0)
    df_allpeople_a = pd.merge(df_allpeople, df_afterp, how='left',
                              on=['base_age', 'base_gender', 'city_level', 'on_price', 'is_exposed']).fillna(0)
    # Modify the column name to start processing
    df_allpeople_b.columns = ['A', 'B', 'C', 'D', 'E', 'X1', 'F', 'X2']
    df_allpeople_n.columns = ['A', 'B', 'C', 'D', 'E', 'X1', 'F', 'X2']
    df_allpeople_a.columns = ['A', 'B', 'C', 'D', 'E', 'X1', 'F', 'X2']
    sumall = df_merge1['count'].sum() + df_merge0['count'].sum()

    df_allpeople_b = df_allpeople_b.drop(columns=['F'])
    df_allpeople_n = df_allpeople_n.drop(columns=['F'])
    df_allpeople_a = df_allpeople_a.drop(columns=['F'])
    df_allpeople_b_psm = get_result(df_allpeople_b, sumall)
    df_allpeople_n_psm = get_result(df_allpeople_n, sumall)
    df_allpeople_a_psm = get_result(df_allpeople_a, sumall)
    # The output result needs to be divided by the total number of people
    list.append(df_allpeople_b_psm[df_allpeople_b_psm['E'] == 1]['psm'].sum())
    list.append(df_allpeople_b_psm[df_allpeople_b_psm['E'] == 0]['psm'].sum())
    list.append(df_allpeople_n_psm[df_allpeople_n_psm['E'] == 1]['psm'].sum())
    list.append(df_allpeople_n_psm[df_allpeople_n_psm['E'] == 0]['psm'].sum())
    list.append(df_allpeople_a_psm[df_allpeople_a_psm['E'] == 1]['psm'].sum())
    list.append(df_allpeople_a_psm[df_allpeople_a_psm['E'] == 0]['psm'].sum())
    return list


def three_time_message(appname):
    # Get the number of strata in each time period
    hql = """
    select 
    base_age,
    base_gender,
    city_level,
    on_price,
    is_exposed,
    time,
    count(distinct B.imei)
    from people_portrait A
    inner join 281_APP_use B
    on A.imei = B.imei
    and B.brand_name = '""" + appname + """'
    group by 
    base_age,
    base_gender,
    city_level,
    on_price,
    is_exposed,
    time
    """
    df = spark.sql(hql)
    return df


def get_result(df_allpeople_b, sumall):
    # calculation
    add1 = []
    for i in range(df_allpeople_b.iloc[:, 0].size / 2):
        x1 = 2 * i
        x2 = x1 + 1
        y1 = float((df_allpeople_b.ix[x1, 'X1'] + df_allpeople_b.ix[x2, 'X1']))
        y2 = float(df_allpeople_b.ix[x1, 'X2'] / df_allpeople_b.ix[x1, 'X1'])
        y3 = float(df_allpeople_b.ix[x2, 'X2'] / df_allpeople_b.ix[x2, 'X1'])
        add1.append(y2 * y1 / (sumall))
        add1.append(y3 * y1 / (sumall))
    df_allpeople_b['psm'] = add1
    return df_allpeople_b


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
    # Header format
    list = ['appname', 'expose_layer_num', 'noexpose_layer_num', 'use_layer_num', 'expose1_people', 'expose0_people',
            'before1', 'before0', 'now1', 'now0', 'after1', 'after0']
    result = pd.DataFrame(columns=list, dtype=object)
    count = 0
    line = get_appname()
    # Incoming app by app for analysis
    for i in line:
        y = []
        count += 1
        y.append(i)
        list2 = stratifying_did(i, y)
        result = result.append(dict(zip(list, list2)), ignore_index=True)
    result.to_csv('result.csv', index=False)