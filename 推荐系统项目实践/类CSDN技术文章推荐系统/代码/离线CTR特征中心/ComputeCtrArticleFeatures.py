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

sqlContext.sql("use article")
article_profile = sqlContext.sql("select * from article_profile")

def article_profile_to_feature(row):
    try:
        article_weights = row.keywords.values()
        weights = sorted(article_weights)[:5]
        weights = weights + [0.0] * (10 - len(weights))
        # print((weights))
    except Exception as e:
        weights = [0.0] * 5
        print(e)
    return row.article_id, row.channel_id, weights

article_profile = article_profile.rdd.map(article_profile_to_feature).toDF(['article_id', 'channel_id', 'weights'])
# article_profile.show()

article_vector = sqlContext.sql("select * from article_vector")
article_feature = article_profile.join(article_vector, on=['article_id'], how='inner')

def feature_to_vector(row):
    from pyspark.ml.linalg import Vectors
    return row.article_id, row.channel_id, Vectors.dense(row.weights), Vectors.dense(row.articlevector)

article_feature = article_feature.rdd.map(feature_to_vector).toDF(['article_id', 'channel_id', 'weights', 'articlevector'])

from pyspark.ml.feature import VectorAssembler
columns = ['article_id', 'channel_id', 'weights', 'articlevector']
article_feature_two = VectorAssembler().setInputCols(columns[1:4]).setOutputCol("features").transform(article_feature)
# article_feature_two.show()

def save_article_feature_to_hbase(partition):
    import happybase
    conn = happybase.Connection('localhost')
    table = conn.table('ctr_feature_article')
    for row in partition:
        table.put('{}'.format(row.article_id).encode(),
                 {'article:{}'.format(row.article_id).encode(): str(row.features).encode()})

article_feature_two.foreachPartition(save_article_feature_to_hbase)


sc.stop()

