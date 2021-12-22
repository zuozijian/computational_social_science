from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, SparseVector
from pyspark.ml.clustering import LDA
import datetime
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import IntegerType, DoubleType, StringType, ArrayType, MapType
from pyspark.sql.functions import split, explode, concat, concat_ws


def get_spark_sess(master="local[*]", deploy_mode='client'):
    spark_sess = SparkSession.builder.master(master).config(
        "spark.submit.deployMode", deploy_mode).enableHiveSupport().getOrCreate()
    spark_sess.sparkContext.setLogLevel("OFF")
    print("get spark session done.")
    return spark_sess


spark = get_spark_sess()


def feature_converter(df):
    vecAss = VectorAssembler(inputCols=df.columns[1:], outputCol='features')
    df_va = vecAss.setHandleInvalid("skip").transform(df)
    return df_va


def lda_Clustering():
    # Get the prepared data sheet
    hql = """
    select * from
    $city_lda_data_log_daynum
                         """
    df = spark.sql(hql)
    test_data = feature_converter(df)
    nowanalyse = 'daynum'
    train_data = test_data.select(nowanalyse, 'features')

    # Load split udf function
    def imeiget(daynum):
        return daynum.split('-')[0]

    def daynoget(daynum):
        return daynum.split('-')[1]

    analysis_fun = udf(imeiget, StringType())
    analysis_fun2 = udf(daynoget, StringType())

    # 1.Run the lda model
    lllp = []
    # Set the number of topics
    for nowk in range(3, 21):
        maxit = 50
        # Model training
        lda = LDA(k=nowk, seed=36, maxIter=maxit, optimizer="online")
        model = lda.fit(train_data)

        # Output topic and category relationship
        topics = model.describeTopics(20)
        topics.registerTempTable('topics')
        detail_max = """create table city_topictype_""" + str(nowk) + """_""" + str(maxit) + """ as
                        select * from topics"""
        spark.sql(detail_max)

        # Output Perplexity value
        lp = model.logPerplexity(train_data)
        ll = model.logLikelihood(train_data)
        lllp.append([lp, ll])
        transformed = model.transform(train_data)

        # Save dayno table
        out = transformed.select('daynum', 'topicDistribution')
        out.registerTempTable('out')
        detail_max = """create table city_imeiframe1_""" + str(nowk) + """_""" + str(maxit) + """ as
                        select * from out"""
        spark.sql(detail_max)

        # Extract dayno and topic values
        hql = """
        create table city_imeiframe2_""" + str(nowk) + """_""" + str(maxit) + """ as
        select daynum,
        topicdistribution['values'] as values
        from city_imeiframe1_""" + str(nowk) + """_""" + str(maxit) + """
                             """
        spark.sql(hql)

        # Separate imei and dayno, save the topic category of imei in each day
        hql = """
        select * from
        city_imeibiao2_""" + str(nowk) + """_""" + str(maxit) + """
                             """
        df = spark.sql(hql)
        # Split daynum to store result
        for j in range(nowk):
            df = df.withColumn("t" + str(j), df["values"][j])
        df2 = df.withColumn('imei', analysis_fun(df.daynum))
        df2 = df2.withColumn('dayno', analysis_fun2(df.daynum))
        df2.registerTempTable('df2')
        detail_max = """create table ad_tmp.2city_imeidayuse_""" + str(nowk) + """_""" + str(maxit) + """ as
                        select * 
                        from df2"""
        spark.sql(detail_max)


def lda_result_get():
    # The proportion of each topic category for obtaining lda results
    for nowk in range(3, 21):
        hql = """
        select * from city_topictype_""" + str(nowk) + """_50
                             """
        topictype = spark.sql(hql)
        Ptopic = topictype.toPandas()
        # Output data according to the output sample
        for i in range(nowk):
            for j in range(20):
                slist = Ptopic.loc[i, "termIndices"]
                term = Ptopic.loc[i, "termWeights"]
                s = slist[j]
                s1 = round(term[j], 2)
                print(str(s1) + '*' + catno[int(s)] + ',', end="")
            print()

    # Get the distribution of the proportions of each category of the two cities by day, and save
    for nowk in range(3, 21):
        s = """select  dayno"""
        for i in range(nowk):
            s += """,sum(t""" + str(i) + """) as t""" + str(i)
        # city1
        hql = s + """  from city_imeidayuse_""" + str(nowk) + """_50
        where where imei >10000
        group by dayno
        order by dayno
                             """
        # city2
        hql2 = s + """  from city_imeidayuse_""" + str(nowk) + """_50
        where where imei <=10000
        group by dayno
        order by dayno
                             """
        city1 = spark.sql(hql)
        city2 = spark.sql(hql2)
        pcity1 = city1.toPandas()
        pcity2 = city2.toPandas()
        pcity1['sum'] = pcity1.iloc[:, 1:].sum(axis=1)
        pcity1['dayno'] = pcity1['dayno']
        # city1
        for i in range(nowk):
            pcity1['topic' + str(i)] = pcity1['t' + str(i)] / pcity1['sum']
        pcity1out = pcity1.iloc[:, -(nowk + 1):]
        pcity1out.to_csv(str(nowk) + 'city1.csv', index=False)
        # city2
        pcity2['sum'] = pcity2.iloc[:, 1:].sum(axis=1)
        pcity2['dayno'] = pcity2['dayno']
        for i in range(nowk):
            pcity2['topic' + str(i)] = pcity2['t' + str(i)] / pcity2['sum']
        pcity2out = pcity2.iloc[:, -(nowk + 1):]
        pcity2out.to_csv(str(nowk) + 'city2.csv', index=False)


def kmodes_data_get():
    # 将lda结果生成为kmodes模型数据
    hql = """
    select * from
    city_imeiframe2_20_50
                         """
    df = spark.sql(hql)
    # Split the values list, select topic category split according to actual needs
    df = df.withColumn("t0", df["values"][0])
    df = df.withColumn("t1", df["values"][1])
    df = df.withColumn("t2", df["values"][2])
    df = df.withColumn("t3", df["values"][3])
    df = df.withColumn("t4", df["values"][4])
    df = df.withColumn("t5", df["values"][5])
    df = df.withColumn("t6", df["values"][6])
    df = df.withColumn("t7", df["values"][7])
    df = df.withColumn("t8", df["values"][8])
    df = df.withColumn("t9", df["values"][9])
    df = df.withColumn("t10", df["values"][10])
    df = df.withColumn("t11", df["values"][11])
    df = df.withColumn("t12", df["values"][12])
    df = df.withColumn("t13", df["values"][13])
    df = df.withColumn("t14", df["values"][14])
    df = df.withColumn("t15", df["values"][15])
    df = df.withColumn("t16", df["values"][16])
    df = df.withColumn("t17", df["values"][17])
    df = df.withColumn("t18", df["values"][18])
    df = df.withColumn("t19", df["values"][19])

    # Load the maximum value function and save the serial number of the topic with the largest proportion
    def foundindex(values):
        return values.index(max(values))

    analysis_fun = udf(foundindex, IntegerType())

    df2 = df.withColumn('max1', analysis_fun(df.values))
    df2 = df2.withColumn('imei', analysis_fun(df.daynum))
    df2 = df2.withColumn('dayno', analysis_fun2(df.daynum))

    # Convert to pandas dataframe and extract the required columns
    pddf2 = df2.toPandas()
    pddfimei = pddf[['imei', 'dayno']]
    test = pddfimei
    test['topic'] = pddf2['max1']

    # Convert date column to row
    two_level_index_series = test.set_index(["imei", "dayno"])["topic"]
    new_df = two_level_index_series.unstack()
    new_df = new_df.rename_axis(columns=None)
    new_df = new_df.reset_index()


def kmodes_Clustering():
    # Run kmodes model
    data = new_df
    data = data.drop(labels='imei', axis=1)
    data = data.values  # 转化为数组

    # Category variable settings
    categoricallist = []
    for i in range(151):
        categoricallist.append(i)

    # Use kmode for all category variables to set the number of clusters
    km = kmodes.KModes(n_clusters=30)
    clusters = km.fit_predict(data)

    # Output each cluster center
    for i in km.cluster_centroids_:
        for j in i:
            print(j, end=',')
        print()
    print(km.cost_)  # SSE value of each k value

    # Output the number of people in each category of the city
    cat = []
    for c in clusters:
        # The clustering results are stored in the list
        cat.append(c)
    new_df['cat'] = cat
    test = new_df[['imei', 'cat']]
    # Statistics category information
    test['x1'] = new_df.apply(lambda x: 'city1' if x.imei <= 10000 else 'city2', axis=1)
    test.groupby(['x1', 'cat']).count().reset_index()


def main():
    # Run lda clustering
    lda_Clustering()
    # Count the clustering results according to the prescribed format
    lda_result_get()
    # Convert the result of lda into the data structure required for kmodes clustering
    kmodes_data_get()
    # Run kmodes clustering
    kmodes_Clustering()