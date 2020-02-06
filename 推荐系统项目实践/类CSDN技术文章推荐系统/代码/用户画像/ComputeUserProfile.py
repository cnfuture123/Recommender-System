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
sqlDF = sqlContext.sql("select actiontime, readtime, channelid, param.articleId, param.algorithmCombine, "
                                "param.action, param.userId from user_action where dt = '2019-03-17' ")

# sqlDF.show()

if sqlDF.collect():
    def _compute(row):
        # 进行判断行为类型
        _list = []
        if row.action == "exposure":
            # print(row.articleId)
            if row.articleId.startswith('['):
                for article_id in eval(row.articleId):
                    _list.append(
                        [row.userId, row.actiontime, article_id, row.channelid, False, False, False, True, row.readtime])
            else:
                 _list.append(
                        [row.userId, row.actiontime, int(row.articleId), row.channelid, False, False, False, True, row.readtime])
            return _list
        else:
            class Temp(object):
                shared = False
                clicked = False
                collected = False
                read_time = ""

            _tp = Temp()
            if row.action == "share":
                _tp.shared = True
            elif row.action == "click":
                _tp.clicked = True
            elif row.action == "collect":
                _tp.collected = True
            elif row.action == "read":
                _tp.clicked = True
            else:
                pass
            _list.append(
                [row.userId, row.actiontime, int(row.articleId), row.channelid, _tp.shared, _tp.clicked, _tp.collected,
                 True,
                 row.readtime])
            return _list
    # 进行处理
    # 查询内容，将原始日志表数据进行处理
    _res = sqlDF.rdd.flatMap(_compute)
    data = _res.toDF(["user_id", "action_time","article_id", "channel_id", "shared", "clicked", "collected", "exposure", "read_time"])

# data.show()
old_data = sqlContext.sql("select * from user_article_basic")
new_data = old_data.unionAll(data)

new_data.registerTempTable("temptable")
sqlContext.sql("insert overwrite table user_article_basic select user_id, max(action_time) as action_time, "
        "article_id, max(channel_id) as channel_id, max(shared) as shared, max(clicked) as clicked, "
        "max(collected) as collected, max(exposure) as exposure, max(read_time) as read_time from temptable "
        "group by user_id, article_id")

sc.stop()

