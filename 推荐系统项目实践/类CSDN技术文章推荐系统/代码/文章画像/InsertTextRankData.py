from pyspark import SparkConf,SparkContext,HiveContext
from pyspark.ml.feature import CountVectorizerModel, IDFModel
import os
import warnings
warnings.filterwarnings('ignore')

os.environ['SPARK_HOME']="C:\Software\spark\spark-2.1.1-bin-hadoop2.7"
os.environ['PYSPARK_PYTHON']= "D:\Python_Tools\Anaconda\python.exe"

sparkConf = SparkConf().setMaster("local[*]").setAppName("ComputeTFIDF")
sc = SparkContext(conf = sparkConf)
sqlContext = HiveContext(sc)

# 分词
def textrank(partition):
    import os

    import jieba
    import jieba.analyse
    import jieba.posseg as pseg
    import codecs

    abspath = "D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/resources"

    # 结巴加载用户词典
    userDict_path = os.path.join(abspath, "ITKeywords.txt")
    jieba.load_userdict(userDict_path)

    # 停用词文本
    stopwords_path = os.path.join(abspath, "stopwords.txt")

    def get_stopwords_list():
        """返回stopwords列表"""
        stopwords_list = [i.strip()
                          for i in codecs.open(stopwords_path, 'r',encoding='UTF-8').readlines()]
        return stopwords_list

    # 所有的停用词列表
    stopwords_list = get_stopwords_list()

    class TextRank(jieba.analyse.TextRank):
        def __init__(self, window=20, word_min_len=2):
            super(TextRank, self).__init__()
            self.span = window  # 窗口大小
            self.word_min_len = word_min_len  # 单词的最小长度
            # 要保留的词性，根据jieba github ，具体参见https://github.com/baidu/lac
            self.pos_filt = frozenset(
                ('n', 'x', 'eng', 'f', 's', 't', 'nr', 'ns', 'nt', "nw", "nz", "PER", "LOC", "ORG"))

        def pairfilter(self, wp):
            """过滤条件，返回True或者False"""

            if wp.flag == "eng":
                if len(wp.word) <= 2:
                    return False

            if wp.flag in self.pos_filt and len(wp.word.strip()) >= self.word_min_len \
                    and wp.word.lower() not in stopwords_list:
                return True
    # TextRank过滤窗口大小为5，单词最小为2
    textrank_model = TextRank(window=5, word_min_len=2)
    allowPOS = ('n', "x", 'eng', 'nr', 'ns', 'nt', "nw", "nz", "c")

    for row in partition:
        tags = textrank_model.textrank(row.sentence, topK=20, withWeight=True, allowPOS=allowPOS, withFlag=False)
        for tag in tags:
            yield row.article_id, row.channel_id, tag[0], tag[1]

# 分词
sqlContext.sql("use article")
articleDF = sqlContext.sql("select * from article_data")

textrankDF = articleDF.rdd.mapPartitions(textrank).toDF(["article_id", "channel_id", "keyword", "textrank"])

# textrankDF.show()
# textrankDF.write.insertInto("textrank_keywords_values")

# merge TFIDF and TextRank result
idf = sqlContext.sql("select * from idf_keywords_values")
idf = idf.withColumnRenamed("keyword", "keyword1")

result = textrankDF.join(idf,textrankDF.keyword==idf.keyword1)
keywords_res = result.withColumn("weights", result.textrank * result.idf).select(["article_id", "channel_id", "keyword", "weights"])

keywords_res.registerTempTable("temptable")
merge_keywords = sqlContext.sql("select article_id, min(channel_id) channel_id, "
                               "collect_list(keyword) keywords, collect_list(weights) weights "
                               "from temptable group by article_id")

# 合并关键词权重合并成字典
def _func(row):
    return row.article_id, row.channel_id, dict(zip(row.keywords, row.weights))

keywords_info = merge_keywords.rdd.map(_func).toDF(["article_id", "channel_id", "keywords"])

topic_sql = """
                select t.article_id aid, collect_set(t.keyword) topics from tfidf_keywords_values t
                inner join 
                textrank_keywords_values r
                where t.keyword = r.keyword
                group by t.article_id
                """
article_topics = sqlContext.sql(topic_sql)

# article_topics.show()

article_profile = keywords_info.join(article_topics, keywords_info.article_id==article_topics.aid).select(
    ["article_id", "channel_id", "keywords", "topics"])

article_profile.write.insertInto("article_profile")

sc.stop()

