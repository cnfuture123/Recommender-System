from pyspark import SparkConf,SparkContext,HiveContext
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
import os
import warnings
warnings.filterwarnings('ignore')

os.environ['SPARK_HOME']="C:\Software\spark\spark-2.4.4-bin-hadoop2.7"
os.environ['PYSPARK_PYTHON']= "D:\Python_Tools\Anaconda\python.exe"

sparkConf = SparkConf().setMaster("local[*]").setAppName("ComputeTFIDF").set("spark.executor.memory", "1g")
# sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
sc = SparkContext(conf = sparkConf)
sqlContext = HiveContext(sc)

sqlContext.sql("use profile")
user_article_ = sqlContext.sql("select * from user_article_basic").drop("channel_id")
# user_article_.show()
sqlContext.sql("use article")
article_label = sqlContext.sql("select article_id, channel_id, topics from article_profile")

user_topic = user_article_.join(article_label, how='left', on=['article_id'])

import pyspark.sql.functions as F
user_topic = user_topic.withColumn('topic', F.explode('topics')).drop('topics')

# 计算每个用户对每篇文章的标签的权重
def compute_user_label_weights(partitions):
    """# 计算用户关键词权重
    """
    weightsOfaction = {
        "read_min": 1,
        "read_middle": 2,
        "collect": 2,
        "share": 3,
        "click": 1
    }

    # 导入包
    from datetime import datetime
    import numpy as np

    # 循环每个用户对应每个关键词处理
    for row in partitions:

        # 计算时间系数
        t = datetime.now() - datetime.strptime(row.action_time, '%Y-%m-%d %H:%M:%S')
        alpha = 1 / (np.log(t.days + 1) + 1)

        # 判断一下这个关键词对应的操作文章时间大小的权重处理
        if row.read_time  == '':
            read_t = 0
        else:
            read_t = int(row.read_time)

        # 阅读时间的行为分数计算出来
        read_score = weightsOfaction['read_middle'] if read_t > 1000 else weightsOfaction['read_min']

        # 计算row.topic的权重
        weights = alpha * (row.shared * weightsOfaction['share'] + row.clicked * weightsOfaction['click'] +
                          row.collected * weightsOfaction['collect'] + read_score)

        yield row.user_id, row.channel_id, row.topic, round(float(weights), 4)


user_topic = user_topic.rdd.mapPartitions(compute_user_label_weights).toDF(["user_id", "channel_id", "topic", "weights"])
# user_topic.show()
sqlContext.sql("use profile")
user_topic.write.insertInto("user_profile_topic")

sc.stop()

