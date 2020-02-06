from pyspark import SparkConf,SparkContext,HiveContext
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
wordsDF = articleDF.rdd.mapPartitions(segmentation).toDF(["article_id", "channel_id", "words"])

# 词语与词频统计
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel, IDF
# 总词汇的大小，文本中必须出现的次数
cv = CountVectorizer(inputCol="words", outputCol="countFeatures", vocabSize=200*10000, minDF=1.0)
# 训练词频统计模型
cv_model = cv.fit(wordsDF)
# cv_model.write().overwrite().save("hdfs://192.168.66.132:9000/headlines/models/CV.model")
cv_model.write().overwrite().save("D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/output/CV.model")

# 词语与词频统计
cv_model = CountVectorizerModel.load("D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/output/CV.model")
# 得出词频向量结果
cv_result = cv_model.transform(wordsDF)
# 训练IDF模型
idf = IDF(inputCol="countFeatures", outputCol="idfFeatures")
idfModel = idf.fit(cv_result)
idfModel.write().overwrite().save("D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/output/IDF.model")

# print(cv_model.vocabulary)
# sentence_df.write.insertInto("article_data")
# sentence_df.show()
#sentence_df.write.option("delimiter", "|").format("csv").save("../output/article")


sc.stop()

