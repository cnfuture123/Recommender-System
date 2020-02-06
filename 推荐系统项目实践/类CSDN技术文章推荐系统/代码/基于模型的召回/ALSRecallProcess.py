from pyspark import SparkConf,SparkContext,HiveContext
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
import os
import warnings
warnings.filterwarnings('ignore')

os.environ['SPARK_HOME']="C:\Software\spark\spark-2.4.4-bin-hadoop2.7"
os.environ['PYSPARK_PYTHON']= "D:\Python_Tools\Anaconda\python.exe"

sparkConf = SparkConf().setMaster("local[*]").setAppName("ALSRecall").set("spark.executor.memory", "1g")
sc = SparkContext(conf = sparkConf)
sqlContext = HiveContext(sc)

user_article_basic = sqlContext.read.load("D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/output/ALS")
# user_article_basic.show()

from pyspark.ml.recommendation import ALS

als = ALS(userCol="user_id", itemCol="article_id", ratingCol="clicked", checkpointInterval=1)
model = als.fit(user_article_basic)
recall_res = model.recommendForAllUsers(10)

# 对于文章推荐的解析
import pyspark.sql.functions as F

recall_res = recall_res.withColumn('als_article_id', F.explode('recommendations')).drop('recommendations').select(['user_id', 'als_article_id'])

def get_article_index(row):
    return row.user_id, row.als_article_id[0]

recall_res = recall_res.rdd.map(get_article_index).toDF(['user_id', 'article_id'])
# recall_res.show()

sqlContext.sql("use article")
article_data = sqlContext.sql("select article_id, channel_id from article_data")

recall_channel = recall_res.join(article_data, on=['article_id'], how='left')
recall_channel = recall_channel.groupBy(['user_id', 'channel_id']).agg(
    F.collect_list('article_id')).withColumnRenamed('collect_list(article_id)', 'article_list')
# recall_channel.show()

def save_offline_recall_hbase(partition):
            """离线模型召回结果存储
            """
            import happybase
            for row in partition:
                conn = happybase.Connection('192.168.66.132')
                # 获取历史看过的该频道文章
                history_table = conn.table('history_recall')
                # 多个版本
                data = history_table.cells('reco:his:{}'.format(row.user_id).encode(),
                                           'channel:{}'.format(row.channel_id).encode())

                history = []
                if len(data) >= 2:
                    for l in data[:-1]:
                        history.extend(eval(l))
                else:
                    history = []

                # 过滤reco_article与history
                reco_res = list(set(row.article_list) - set(history))

                if reco_res:

                    table = conn.table('cb_recall')
                    # 默认放在推荐频道
                    table.put('recall:user:{}'.format(row.user_id).encode(),
                              {'als:{}'.format(row.channel_id).encode(): str(reco_res).encode()})

                    # 放入历史推荐过文章
                    history_table.put("reco:his:{}".format(row.user_id).encode(),
                                      {'channel:{}'.format(row.channel_id): str(reco_res).encode()})
                conn.close()

recall_channel.foreachPartition(save_offline_recall_hbase)

sc.stop()
