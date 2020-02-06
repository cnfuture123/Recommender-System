import pyspark.sql.functions as F
from pyspark import SparkConf,SparkContext,HiveContext
import os
import warnings
warnings.filterwarnings('ignore')

os.environ['SPARK_HOME']="C:\Software\spark\spark-2.1.1-bin-hadoop2.7"
os.environ['PYSPARK_PYTHON']= "D:\Python_Tools\Anaconda\python.exe"

sparkConf = SparkConf().setMaster("local[*]").setAppName("mergeArticle")
sc = SparkContext(conf = sparkConf)
#sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
sqlContext = HiveContext(sc)

# query article information
sqlContext.sql("use toutiao")
basic_content = sqlContext.sql(
            "select a.article_id, a.channel_id, a.title, b.content "
            "from news_article_basic a inner join news_article_content b on a.article_id=b.article_id ")

basic_content.registerTempTable("temparticle")

# 增加channel的名字，后面会使用
channel_basic_content = sqlContext.sql(
            "select t.*, n.channel_name from temparticle t left join news_channel n "
            "on t.channel_id=n.channel_id")

# 利用concat_ws方法，将多列数据合并为一个长文本内容（频道，标题以及内容合并）
sqlContext.sql("use article")
sentence_df = channel_basic_content.select("article_id", "channel_id", "channel_name", "title", "content",
                                           F.concat_ws(
                                             ",",
                                             channel_basic_content.channel_name,
                                             channel_basic_content.title,
                                             channel_basic_content.content
                                           ).alias("sentence")
                                          )

# sentence_df.write.insertInto("article_data")
sentence_df.show()
# sentence_df.write.option("delimiter", "|").format("csv").save("../output/article")

sc.stop()
