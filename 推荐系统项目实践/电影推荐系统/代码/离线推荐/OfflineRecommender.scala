package cn.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

// 基于评分数据的LFM，只需要rating数据
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Long )

case class MongoConfig(uri:String, db:String)

// 定义一个基准推荐对象
case class Recommendation( mid: Int, score: Double )

// User Recommendation
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// Movie Similarity
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object OfflineRecommender {

    // define table names
    val MONGODB_RATING_COLLECTION = "Rating"
    val USER_RECS = "UserRecs"
    val MOVIE_RECS = "MovieRecs"

    val USER_MAX_RECOMMENDATION = 10

    def main(args: Array[String]): Unit = {
      val config = Map(
        "spark.cores" -> "local[*]",
        "mongo.uri" -> "mongodb://master:27017/recommender",
        "mongo.db" -> "recommender"
      )

      // SparkConf
      val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

      // SparkSession
      val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

      import sparkSession.implicits._

      implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

      // Read data from MongoDB
      val ratingRDD = sparkSession
        .read
        .option("uri", mongoConfig.uri)
        .option("collection", MONGODB_RATING_COLLECTION)
        .format("com.mongodb.spark.sql")
        .load()
        .as[MovieRating]
        .rdd
        .map(rating => (rating.uid, rating.mid, rating.score))
        .cache()

      // construct userRDD and movieRDD
      val userRDD = ratingRDD.map(_._1).distinct()
      val movieRDD = ratingRDD.map(_._2).distinct()

      // Train LFM Model
      val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))

      // rank是模型中隐语义因子的个数, iterations是迭代的次数, lambda是ALS的正则化参数
      val (rank, iterations, lambda) = (200, 10, 0.1)

      // Build model
      val model = ALS.train(trainData, rank, iterations, lambda)

      // User-Movie Matrix
      val userMovies = userRDD.cartesian(movieRDD)

      // Predict ratings
      val predictedRatings = model.predict(userMovies)

      // User Recommendation
      val userRecs = predictedRatings
        .filter(_.rating > 0)
        .map(rating => (rating.user, (rating.product, rating.rating)))
        .groupByKey()
        .map {
          case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION)
            .map(x => Recommendation(x._1, x._2)))
        }
        .toDF()

      // write into MongoDB
      userRecs.write
        .option("uri", mongoConfig.uri)
        .option("collection", USER_RECS)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

      // Calculate movie similarity matrix
      val movieFeatures = model.productFeatures.map{
        case(mid, features) => (mid, new DoubleMatrix(features))
      }

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
        .option("collection", MOVIE_RECS)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

      // close spark session
      sparkSession.close()
  }

  def cosSimilarity(movie1: DoubleMatrix, movie2: DoubleMatrix): Double ={
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}
