from pyspark import SparkConf,SparkContext,HiveContext
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
import os
import warnings
warnings.filterwarnings('ignore')

os.environ['SPARK_HOME']="C:\Software\spark\spark-2.4.4-bin-hadoop2.7"
os.environ['PYSPARK_PYTHON']= "D:\Python_Tools\Anaconda\python.exe"

sparkConf = SparkConf().setMaster("local[*]").setAppName("ContentRecall").set("spark.executor.memory", "1g")
sc = SparkContext(conf = sparkConf)
sqlContext = HiveContext(sc)

sqlContext.sql("use profile")
user_article_basic = sqlContext.sql("select * from user_article_basic")
user_article_basic = user_article_basic.filter("clicked=True")
# user_article_basic.show()

def save_content_filter_history_to__recall(partition):
    """计算每个用户的每个操作文章的相似文章，过滤之后，写入content召回表当中（支持不同时间戳版本）
    """
    import happybase
    conn = happybase.Connection('localhost')
    # key:   article_id,    column:  similar:article_id
    similar_table = conn.table('article_similar')
    # 循环partition
    for row in partition:
        # 获取相似文章结果表
        similar_article = similar_table.row(str(row.article_id).encode(),columns=[b'similar'])
        # 相似文章相似度排序过滤，召回不需要太大的数据， 百个，千
        _srt = sorted(similar_article.items(), key=lambda item: item[1], reverse=True)
        if _srt:
            # 每次行为推荐10篇文章
            reco_article = [int(i[0].split(b':')[1]) for i in _srt][:10]

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
            reco_res = list(set(reco_article) - set(history))

            # 进行推荐，放入基于内容的召回表当中以及历史看过的文章表当中
            if reco_res:
                # content_table = conn.table('cb_content_recall')
                content_table = conn.table('cb_recall')
                content_table.put("recall:user:{}".format(row.user_id).encode(),
                                  {'content:{}'.format(row.channel_id).encode(): str(reco_res).encode()})

                # 放入历史推荐过文章
                history_table.put("reco:his:{}".format(row.user_id).encode(),
                                  {'channel:{}'.format(row.channel_id).encode(): str(reco_res).encode()})

    conn.close()

user_article_basic.foreachPartition(save_content_filter_history_to__recall)

sc.stop()

