from pyspark import SparkConf,SparkContext,HiveContext
import os
import warnings
warnings.filterwarnings('ignore')
import pyspark.sql.functions as F

os.environ['SPARK_HOME']="C:\Software\spark\spark-2.4.4-bin-hadoop2.7"
os.environ['PYSPARK_PYTHON']= "D:\Python_Tools\Anaconda\python.exe"

sparkConf = SparkConf().setMaster("local[*]").setAppName("ContentRecall").set("spark.executor.memory", "1g")
sc = SparkContext(conf = sparkConf)
sqlContext = HiveContext(sc)

# conpute user features
sqlContext.sql("use profile")
user_profile = sqlContext.sql("select * from user_profile_topic")
user_profile = user_profile.groupBy(['user_id', 'channel_id']).agg(F.collect_list('weights')).withColumnRenamed(
    'collect_list(weights)', 'user_weights')

def user_feature_process(row):

    # 取出所有对应particle平道的关键词权重（用户）
    from pyspark.ml.linalg import Vectors
    try:
        user_weights = list(row.user_weights + [0.0] * (10 - len(row.user_weights)))
        weights = sorted(user_weights)[:10]
    except Exception as e:
        weights = [0.0] * 10

    return row.user_id, row.channel_id, weights

user_feature = user_profile.rdd.map(user_feature_process).toDF(['user_id', 'channel_id', 'user_weights'])
user_feature.show()

def save_user_feature_to_hbase(partition):
    import happybase
    conn = happybase.Connection('localhost')
    table = conn.table('ctr_feature_user')
    for row in partition:
        table.put(str(row.user_id).encode(),
                 {"channel:{}".format(row.channel_id).encode(): str(row.user_weights).encode()})
    # 手动关闭所有的连接-
    conn.close()

user_feature.foreachPartition(save_user_feature_to_hbase)

sc.stop()

