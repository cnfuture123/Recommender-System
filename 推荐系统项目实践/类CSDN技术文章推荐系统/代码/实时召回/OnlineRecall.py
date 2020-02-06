from pyspark import SparkConf,SparkContext,HiveContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os
import json
import time
from datetime import datetime
import logging
import warnings
warnings.filterwarnings('ignore')
import pyspark.sql.functions as F

os.environ['SPARK_HOME']="C:\Software\spark\spark-2.4.4-bin-hadoop2.7"
os.environ['PYSPARK_PYTHON']= "D:\Python_Tools\Anaconda\python.exe"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.2 pyspark-shell"

KAFKA_SERVER = "localhost:9092"
logger = logging.getLogger('online')

# create StreamingContext
sparkConf = SparkConf().setMaster("local[*]").setAppName("OnlineRecall").set("spark.executor.memory", "1g")
sc = SparkContext(conf = sparkConf)
streamingContext = StreamingContext(sc, 60)

# kafka读取的配置
# 基于内容召回配置，用于收集用户行为，获取相似文章实时推荐
similar_kafkaParams = {"metadata.broker.list": KAFKA_SERVER, "group.id": 'similar'}
SIMILAR_DS = KafkaUtils.createDirectStream(streamingContext, ['click-trace'], similar_kafkaParams)

class OnlineRecall(object):

    def _update_content_recall(self):
        def get_similar_online_recall(rdd):
            import happybase
            conn = happybase.Connection('localhost')
            for data in rdd.collect():
                if data['param']['action'] in ['click', 'collect', 'share']:
                    logger.info("{} INFO: get user_id:{} action:{}  log".format(
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), data['param']['userId'], data['param']['action']))

                    sim_table = conn.table("article_similar")
                    # 根据用户点击流日志涉及文章找出与之最相似文章(基于内容的相似)，选取TOP-k相似的作为召回推荐结果
                    _sim_data = sim_table.row(str(data['param']['articleId']).encode(), columns=[b'similar'])
                    _sim_article = sorted(_sim_data.items(), key=lambda obj: obj[1], reverse=True)
                    if _sim_article:
                        topKSimArticle = [int(i[0].split(b':')[1]) for i in _sim_article[:10]]
                        # 根据历史推荐集过滤已经给用户推荐过的文章
                        his_table = conn.table("history_recall")
                        _history_data = his_table.cells(
                            b"reco:his:%s" % data["param"]["userId"].encode(),
                            b"channel:%d" % data["channelId"]
                        )

                        history = []
                        if len(_history_data) > 1:
                            for i in _history_data:
                                history.extend(i)

                        # 根据历史召回记录，过滤召回结果
                        recall_list = list(set(topKSimArticle) - set(history))

                        # 如果有推荐结果集，那么将数据添加到cb_recall表中，同时记录到历史记录表中
                        logger.info(
                                "{} INFO: store online recall data:{}".format(
                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), str(recall_list)))

                        if recall_list:
                            recall_table = conn.table("cb_recall")
                            recall_table.put(
                                b"recall:user:%s" % data["param"]["userId"].encode(),
                                {b"online:%d" % data["channelId"]: str(recall_list).encode()}
                            )

                            his_table.put(
                                b"reco:his:%s" % data["param"]["userId"].encode(),
                                {b"channel:%d" % data["channelId"]: str(recall_list).encode()}
                            )
            conn.close()

        SIMILAR_DS.map(lambda x: json.loads(x[1])).foreachRDD(get_similar_online_recall)

        # SIMILAR_DS.map(lambda x: json.loads(x[1])).foreachRDD(get_similar_online_recall)

if __name__ == '__main__':
    onlineRecall = OnlineRecall()
    onlineRecall._update_content_recall()
    streamingContext.start()
        # 使用 ctrl+c 可以退出服务
    _ONE_DAY_IN_SECONDS = 60 * 60 * 24
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        pass

