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

sqlContext.sql("use article")
article_vector = sqlContext.sql("select article_id, articlevector from article_vector")

train_data = article_vector.select(['article_id', 'articlevector'])

def _array_to_vector(row):
    return row.article_id, Vectors.dense(row.articlevector)

train_data = train_data.rdd.map(_array_to_vector).toDF(['article_id', 'articleVector'])

# train_data.show()

from pyspark.ml.feature import BucketedRandomProjectionLSH

brp = BucketedRandomProjectionLSH(inputCol="articleVector", outputCol="hashes", numHashTables=4, bucketLength=10)
model = brp.fit(train_data)

similarity = model.approxSimilarityJoin(train_data, train_data, 5.0, distCol='EuclideanDistance')

def save_hbase(partition):
    import happybase
    conn = happybase.Connection('localhost')
    table = conn.table('article_similar')
    for row in partition:
        if row.datasetA.article_id == row.datasetB.article_id:
            pass
        else:
            table.put(str(row.datasetA.article_id).encode(),
                     {"similar:{}".format(row.datasetB.article_id).encode(): b'%0.4f' % (row.EuclideanDistance)})
    # 手动关闭所有的连接-
    conn.close()

similarity.foreachPartition(save_hbase)

sc.stop()

