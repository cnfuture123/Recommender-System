package cn.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

// Movie class
case class Movie(mid: Int, name: String, desc: String, timelong: String, issue: String, shoot: String,
                 language: String, genres: String, actors: String, directors: String)

case class MongoConfig(uri: String, db: String)

case class Recommendation(mid: Int, score: Double)

// Movie Similarity
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object ContentRecommender {

  // 定义表名和常量
  val MONGODB_MOVIE_COLLECTION = "Movie"

  val CONTENT_MOVIE_RECS = "ContentMovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://master:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据，并作预处理
    val movieTagsDF = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .map{
        x => (x.mid, x.name, x.genres.map(char => if (char == '|') ' ' else char))
      }.toDF("mid", "name", "genres").cache()

    // 创建一个分词器，默认按空格分词
    val tokenizer = new Tokenizer().setInputCol("genres").setOutputCol("words")

    // 用分词器对原始数据做转换，生成新的一列words
    val wordsData = tokenizer.transform(movieTagsDF)

    // 引入HashingTF工具，可以把一个词语序列转化成对应的词频
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featureData = hashingTF.transform(wordsData)

    // 引入IDF工具
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    // 训练idf模型，得到每个词的逆文档频率
    val idfModel = idf.fit(featureData)

    // 用模型对原数据进行处理，得到文档中每个词的tf-idf，作为新的特征向量
    val transformedData = idfModel.transform(featureData)

    transformedData.show()

    val movieFeatures = transformedData.map{
      row => (row.getAs[Int]("mid"), row.getAs[SparseVector]("features").toArray)
    }.rdd
      .map(
        x => (x._1, new DoubleMatrix(x._2))
      )

    // 对所有电影两两计算它们的相似度，先做笛卡尔积
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        // filter itself
        case(a, b) => a._1 != b._1
      }
      .map{
        case(a, b) =>
          val simScore = cosSimilarity(a._2, b._2)
          (a._1, (b._1, simScore))
      }
      .filter(_._2._2 > 0.6) // simScore > 0.6
      .groupByKey()
      .map{
        case(mid, items) => MovieRecs(mid, items.toList.sortWith(_._2 > _._2)
          .map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    // write into MongoDB
    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", CONTENT_MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    sparkSession.close()

  }

  def cosSimilarity(movie1: DoubleMatrix, movie2: DoubleMatrix): Double ={
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}
