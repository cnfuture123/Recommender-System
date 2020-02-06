import pyspark.sql.functions as F
from pyspark import SparkConf,SparkContext,HiveContext
from pyspark.ml.feature import Word2Vec, Word2VecModel
import os
import warnings
warnings.filterwarnings('ignore')

os.environ['SPARK_HOME']="C:\Software\spark\spark-2.4.4-bin-hadoop2.7"
os.environ['PYSPARK_PYTHON']= "D:\Python_Tools\Anaconda\python.exe"

sparkConf = SparkConf().setMaster("local[*]").setAppName("Word2Vec")
sc = SparkContext(conf = sparkConf)
sqlContext = HiveContext(sc)

# 用jieba分词
def segmentation(partition):
    import os
    import re

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

    # 分词
    def cut_sentence(sentence):
        """对切割之后的词语进行过滤，去除停用词，保留名词，英文和自定义词库中的词，长度大于2的词"""
        # print(sentence,"*"*100)
        # eg:[pair('今天', 't'), pair('有', 'd'), pair('雾', 'n'), pair('霾', 'g')]
        seg_list = pseg.lcut(sentence)
        seg_list = [i for i in seg_list if i.flag not in stopwords_list]
        filtered_words_list = []
        for seg in seg_list:
            # print(seg)
            if len(seg.word) <= 1:
                continue
            elif seg.flag == "eng":
                if len(seg.word) <= 2:
                    continue
                else:
                    filtered_words_list.append(seg.word)
            elif seg.flag.startswith("n"):
                filtered_words_list.append(seg.word)
            elif seg.flag in ["x", "eng"]:  # 是自定一个词语或者是英文单词
                filtered_words_list.append(seg.word)
        return filtered_words_list

    for row in partition:
        sentence = re.sub("<.*?>", "", row.sentence)    # 替换掉标签数据
        words = cut_sentence(sentence)
        yield row.article_id, row.channel_id, words

# 分词
sqlContext.sql("use article")
articleDF = sqlContext.sql("select * from article_data")

wordsDF = articleDF.rdd.mapPartitions(segmentation).toDF(["article_id", "channel_id", "words"])

# Train word2vec model
word2vec = Word2Vec(vectorSize=50, inputCol="words", outputCol="model", minCount=2)
model = word2vec.fit(wordsDF)
model.save("D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/output/Word2Vec.model")

# Load the model
wv_model = Word2VecModel.load("D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/output/Word2Vec.model")
vectors = wv_model.getVectors()

profile = sqlContext.sql("select * from article_profile")
profile.registerTempTable("incremental")
articleKeywordsWeights = sqlContext.sql("select article_id, channel_id, keyword, weight "
                                        "from incremental LATERAL VIEW explode(keywords) AS keyword, weight")
_article_profile = articleKeywordsWeights.join(vectors, vectors.word==articleKeywordsWeights.keyword, "inner")

articleKeywordVectors = _article_profile.rdd.map(
    lambda row: (row.article_id, row.channel_id, row.keyword, row.weight * row.vector)).toDF(
    ["article_id", "channel_id", "keyword", "weightingVector"])

def avg(row):
    x = 0
    for v in row.vectors:
        x += v
    #  将平均向量作为article的向量
    return row.article_id, row.channel_id, x / len(row.vectors)

articleKeywordVectors.registerTempTable("tempTable")
articleVector = sqlContext.sql(
    "select article_id, min(channel_id) channel_id, collect_set(weightingVector) vectors "
    "from tempTable group by article_id").rdd.map(
    avg).toDF(["article_id", "channel_id", "articleVector"])

def toArray(row):
    return row.article_id, row.channel_id, [float(i) for i in row.articleVector.toArray()]

articleVector = articleVector.rdd.map(toArray).toDF(['article_id', 'channel_id', 'articleVector'])

# articleVector.write.insertInto("article_vector")

sc.stop()
