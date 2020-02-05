package cn.offline

import breeze.numerics.sqrt
import cn.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TrainALSModel {

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
      .map(rating => Rating(rating.uid, rating.mid, rating.score))
      .cache()

    // split dataset
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainRDD = splits(0)
    val testRDD = splits(1)

    // get optimal parameters
    adjustALSParams(trainRDD, testRDD)

    // close spark session
    sparkSession.close()

  }

  def adjustALSParams(trainRDD: RDD[Rating], testRDD: RDD[Rating]): Unit={
    val result = for(rank <- Array(50, 100, 150, 200); lambda <- Array(0.01, 0.05, 0.1, 0.02))
      yield{
        val model = ALS.train(trainRDD, rank, 10, lambda)
        val rmse = calRMSE(model, testRDD)
        (rank, lambda, rmse)
      }
    // print result
    println("Optimal Parameters are: " + result.minBy(_._3))
  }

  def calRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double= {
    val userMovies = data.map(item => (item.user, item.product))
    val prediction = model.predict(userMovies)
    // convert format to compare
    val realRating = data.map(item => ((item.user, item.product), item.rating))
    val predictedRating = prediction.map(item => ((item.user, item.product), item.rating))
    sqrt {
      realRating.join(predictedRating).map {
        case ((uid, mid), (realRating, predictedRating)) =>
          val error = realRating - predictedRating
          error * error
      }.mean()
    }
  }

}
