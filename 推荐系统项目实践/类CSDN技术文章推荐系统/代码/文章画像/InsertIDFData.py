from pyspark.ml.feature import CountVectorizerModel, IDFModel
from pyspark import SparkConf,SparkContext,HiveContext

sparkConf = SparkConf().setMaster("local[*]").setAppName("InsertData")
sc = SparkContext(conf = sparkConf)
sqlContext = HiveContext(sc)

cv_model = CountVectorizerModel.load("D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/output/CV.model")
idf_model = IDFModel.load("D:/WorkSpace/ToutiaoRecommenderWorkSpace/toutiao_project/reco_sys/output/IDF.model")

def func(data):
   for index in range(len(data)):
       data[index] = list(data[index])
       data[index].append(index)
       data[index][1] = float(data[index][1])

keywords_list_with_idf = list(zip(cv_model.vocabulary, idf_model.idf.toArray()))
func(keywords_list_with_idf)
rdd = sc.parallelize(keywords_list_with_idf)
df = rdd.toDF(["keywords", "idf", "index"])

sqlContext.sql("use article")
df.write.insertInto('idf_keywords_values')
