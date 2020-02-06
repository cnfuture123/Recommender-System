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

# words_df = article_dataframe.rdd.mapPartitions(segmentation).toDF(["article_id", "channel_id", "words"])
wordsDF = articleDF.rdd.mapPartitions(segmentation, 5).toDF(["article_id", "channel_id", "words"])

cv_model = CountVectorizerModel.load("D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/output/CV.model")
idf_model = IDFModel.load("D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/output/IDF.model")

cv_result = cv_model.transform(wordsDF)
tfidf_result = idf_model.transform(cv_result)

def func(partition):
    TOPK = 20
    for row in partition:
        # 找到索引与IDF值并进行排序
        _ = list(zip(row.idfFeatures.indices, row.idfFeatures.values))
        _ = sorted(_, key=lambda x: x[1], reverse=True)
        result = _[:TOPK]
        for word_index, tfidf in result:
            yield row.article_id, row.channel_id, int(word_index), round(float(tfidf), 4)

_keywordsByTFIDF = tfidf_result.rdd.mapPartitions(func).toDF(["article_id", "channel_id", "index", "tfidf"])

# 利用结果索引与”idf_keywords_values“合并关键词
keywordsIndex = sqlContext.sql("select keyword, index idx from idf_keywords_values")
# 利用结果索引与”idf_keywords_values“合并关键词
keywordsByTFIDF = _keywordsByTFIDF.join(keywordsIndex, keywordsIndex.idx == _keywordsByTFIDF.index).select(["article_id", "channel_id", "keyword", "tfidf"])
keywordsByTFIDF.write.insertInto("tfidf_keywords_values")
# keywordsByTFIDF.show()

sc.stop()

