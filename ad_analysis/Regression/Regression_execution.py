from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
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


def regression(appname, appnam):
    # Regression result output
    list = ['appname', 'c1', 'se1', 'c2', 'se2']
    result = pd.DataFrame(columns=list, dtype=object)
    allappstr = ''
    hqlstr = 'on_price1000,on_price6000,base_age18_24,base_age25_29,base_age30_34,base_age35_49,base_age_17,base_age_50,base_gender_woman,base_gender_man,city_level_1,city_level_3,city_level_2,city_level_new1,on_price_1000_2000,on_price_2000_3000,on_price_3000_4000,on_price_4000_5000,on_price_5000_6000,'
    xstr = """
            is_expose,
            (case when beforeuse is null then 0 else beforeuse end) as beforeuse,
            (case when beforedown is null then 0  else beforedown end) as beforedown,
            (case when beforesearch is null then 0 else beforesearch end) as beforesearch,
            (case
             when GH is null and GL is null then 0 
             when GH is null and GL is not null then GL
             when GL is null and GH is not null then GH
             when GL is not null and GH is not null then GH*GL
             else 0 end) as Guse,
            (case
             when H is null and L is null then 0 
             when H is null and L is not null then L
             when L is null and H is not null then H
             when L is not null and H is not null then H*L
             else 0 end) as NGuse,
            (case
             when GH is null and H is null then 0 
             when GH is null and H is not null then H
             when H is null and GH is not null then GH
             when H is not null and GH is not null then GH*H
             else 0 end) as Huse,
             (case
             when L is null and GL is null then 0 
             when L is null and GL is not null then GL
             when GL is null and L is not null then L
             when GL is not null and L is not null then L*GL
             else 0 end) as Luse,
            """
    y = 'nowuse,afteruse'
    hqlstr = xstr + hqlstr
    hqlstr = hqlstr + y
    hql = """select """ + hqlstr + """ from ad_tag.wangyu_partition_allapp_allmessagereal
    where appname = '""" + str(appname) + """'
    """
    # Get the required data
    df = spark.sql(hql)

    # Eigenvector conversion function
    def feature_converter(df):
        vecAss = VectorAssembler(inputCols=df.columns[0:-2], outputCol='features')
        df_va = vecAss.setHandleInvalid("skip").transform(df)
        return df_va

    test_data = feature_converter(df)
    nowanalyse = 'nowuse'
    train_data = test_data.select(nowanalyse, 'features')
    # Start regression, set relevant values
    lr = LinearRegression(labelCol=nowanalyse, solver="normal")
    lr.setElasticNetParam(1.0)
    lr.setRegParam(0.0)
    lr_model = lr.fit(train_data)
    trainingSummary = lr_model.summary
    try:
        # Store regression results
        dfg = pd.DataFrame()
        c1 = []
        se1 = []
        app = []
        x = trainingSummary.coefficientStandardErrors
        r2_1 = trainingSummary.r2
        c1.append(lr_model.intercept)
        se1.append(trainingSummary.coefficientStandardErrors[len(lr_model.coefficients)])
        app.append(appnam)
        for i in range(len(lr_model.coefficients)):
            c1.append(lr_model.coefficients[i])
            se1.append(trainingSummary.coefficientStandardErrors[i])
            app.append(appnam)
        dfg['appname'] = app
        dfg['c1'] = c1
        dfg['se1'] = se1
    except:
        return result
    # The time period after launch is similar to before
    test_data = feature_converter(df)
    nowanalyse = 'afteruse'
    train_data = test_data.select(nowanalyse, 'features')
    lr = LinearRegression(labelCol=nowanalyse, solver="normal")
    lr.setElasticNetParam(1.0)
    lr.setRegParam(0.0)
    lr_model = lr.fit(train_data)
    trainingSummary = lr_model.summary
    try:
        c1 = []
        se1 = []
        x = trainingSummary.coefficientStandardErrors
        r2_2 = trainingSummary.r2
        c1.append(lr_model.intercept)
        se1.append(trainingSummary.coefficientStandardErrors[len(lr_model.coefficients)])
        for i in range(len(lr_model.coefficients)):
            c1.append(lr_model.coefficients[i])
            se1.append(trainingSummary.coefficientStandardErrors[i])
        dfg['c2'] = c1
        dfg['se2'] = se1
    except:
        return result
    print(appname, r2_1, r2_2)
    return dfg


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
    list = ['appname', 'c1', 'se1', 'c2', 'se2']
    applist = get_appname()
    result = pd.DataFrame(columns=list, dtype=object)
    for i in range(281):
        count = i
        list2 = regression(applist[count], applist[count])
        result = result.append(list2)
    return result