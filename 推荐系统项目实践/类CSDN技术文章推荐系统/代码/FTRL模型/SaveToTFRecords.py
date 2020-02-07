from pyspark import SparkConf,SparkContext,HiveContext
from pyspark.ml.feature import VectorAssembler
import os
import warnings
warnings.filterwarnings('ignore')
import pyspark.sql.functions as F
import pandas as pd
import tensorflow as tf

os.environ['SPARK_HOME']="C:\Software\spark\spark-2.4.4-bin-hadoop2.7"
os.environ['PYSPARK_PYTHON']= "D:\Python_Tools\Anaconda\python.exe"

sparkConf = SparkConf().setMaster("local[*]").setAppName("ContentRecall").set("spark.executor.memory", "1g")
sc = SparkContext(conf = sparkConf)
sqlContext = HiveContext(sc)

sqlContext.sql("use profile")
user_article_basic = sqlContext.sql("select user_id, article_id, clicked from user_article_basic")
user_profile = sqlContext.sql("select user_id, topic, weights from user_profile_topic")
user_profile = user_profile.groupBy(['user_id']).agg(F.collect_list('weights')).withColumnRenamed(
    'collect_list(weights)', 'user_weights')

train_data = user_article_basic.join(user_profile, on=['user_id'], how='left')

sqlContext.sql("use article")
article_vector = sqlContext.sql("select * from article_vector")

train_data = train_data.join(article_vector, on=['article_id'], how='left')
# train_data.show()
train_data = train_data.dropna()

article_profile = sqlContext.sql("select article_id, keywords from article_profile")
# 处理文章权重
def get_article_weights(row):
    try:
        article_weights = list(row.keywords.values() + [0] * (10 - len(row.keywords.values())))
        weights = sorted(article_weights)[:10]
    except Exception as e:
        # 给定异常默认值
        weights = [0.0] * 10
    return row.article_id, weights

article_profile = article_profile.rdd.map(get_article_weights).toDF(['article_id', 'article_weights'])
# article_profile.show()
train_data = train_data.join(article_profile, on=['article_id'], how='left')
train_data = train_data.dropna()
# train_data.show()
columns = ['article_id', 'user_id', 'channel_id', 'articlevector', 'user_weights', 'article_weights', 'clicked']
# array --->vecoter
def get_user_weights(row):

    # 取出所有对应particle平道的关键词权重（用户）
    from pyspark.ml.linalg import Vectors
    try:
        user_weights = list(row.user_weights + [0] * (10 - len(row.user_weights)))
        weights = sorted(user_weights)[:10]
    except Exception as e:
        weights = [0.0] * 10
    return row.article_id, row.user_id, row.channel_id, Vectors.dense(row.articlevector), Vectors.dense(weights), Vectors.dense(row.article_weights),int(row.clicked)

train_vector = train_data.rdd.map(get_user_weights).toDF(columns)
train = VectorAssembler().setInputCols(columns[2:6]).setOutputCol("features").transform(train_vector)
# train.show()

df = train.select(['user_id', 'article_id', 'clicked', 'features'])
df_array = df.collect()
df = pd.DataFrame(df_array)

def write_to_tfrecords(click_batch, feature_batch):
    # initialize writer
    writer = tf.io.TFRecordWriter("D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/output/TFRecords")

    # 循环将所有样本一个个封装成example，写入这个文件
    for i in range(len(click_batch)):
        click = click_batch[i]
        feature = feature_batch[i].tostring()
        print(len(feature))
        # construct example
        example = tf.train.Example(features=tf.train.Features(
            feature={
                "label" : tf.train.Feature(int64_list=tf.train.Int64List(value=[click])),
                "feature" : tf.train.Feature(bytes_list=tf.train.BytesList(value=[feature]))
            }
        ))

        # 序列化example,写入文件
        writer.write(example.SerializeToString())
    writer.close()

# 开启会话打印内容

with tf.compat.v1.Session() as sess:
    coord = tf.train.Coordinator()
    threads = tf.compat.v1.train.start_queue_runners(sess=sess, coord=coord)
    write_to_tfrecords(df.iloc[:, 2], df.iloc[:, 3])
    coord.request_stop()
    coord.join(threads)
