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

sqlContext.sql("use profile")
user_article_basic = sqlContext.sql("select user_id, article_id, clicked from user_article_basic")

def change_types(row):
    return row.user_id, row.article_id, int(row.clicked)

user_article_basic = user_article_basic.rdd.map(change_types).toDF(['user_id', 'article_id', 'clicked'])
user_article_basic.write.save("D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/output/ALS")


sc.stop()

