import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

// Movie class
case class Movie(mid: Int, name: String, desc: String, timelong: String, issue: String, shoot: String,
                 language: String, genres: String, actors: String, directors: String)
// Rating class
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Long)

case class MongoConfig(uri: String, db: String)

case class Recommendation(mid: Int, score: Double)

case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object StatisticsRecommender {

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  // statistics table name
  val HISTORY_POP_MOVIES = "HistoryPopMovies"
  val RECENT_POP_MOVIES = "RecentPopMovies"
  val AVERAGE_MOVIE_SCORE = "AverageMovieScore"
  val GENRES_TOP10_MOVIES = "GenresTop10Movies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://master:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // SparkConf
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))

    // SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import sparkSession.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // Read data from MongoDB
    val ratingDF = sparkSession
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = sparkSession
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    // Create a temporary view
    ratingDF.createOrReplaceTempView("ratings")

    // Statistics Recommendation
    // 1.History Popular Movies --> (mid, count)
    val historyPopMoviesDF = sparkSession.sql("select mid, count(mid) as count from ratings group by mid " +
      "order by count desc")
    // write into MongoDB
    storeDFInMongoDB(historyPopMoviesDF, HISTORY_POP_MOVIES)

    // 2.Recent Popular Movies --> (mid, count, time(yyyyMM))
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")

    // register a UDF function "changeDate" to convert timestamp --> "yyyyMM"
    sparkSession.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    val ratingOfYearMonth = sparkSession.sql("select mid, score, changeDate(timestamp) " +
      "as ratingMonth from ratings")

    // create a temporary view
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    val recentPopMoviesDF = sparkSession.sql("select mid, count(mid) as count, ratingMonth " +
      "from ratingOfMonth group by ratingMonth, mid order by ratingMonth desc, count desc")

    // write into MongoDB
    storeDFInMongoDB(recentPopMoviesDF, RECENT_POP_MOVIES)

    // 3.Average Movie Score --> (mid, avgScore)
    val avgMovieScoreDF = sparkSession.sql("select mid, avg(score) as avg from ratings group by mid " +
      "order by avg desc")

    // write into MongoDB
    storeDFInMongoDB(avgMovieScoreDF, AVERAGE_MOVIE_SCORE)

    // 4.Genres Top 10 Movies
    // define all genres
    val genres = List("Action","Adventure","Animation","Comedy","Crime","Documentary","Drama","Family","Fantasy",
      "Foreign", "History","Horror","Music","Mystery","Romance","Science","Tv","Thriller","War","Western")

    // join movie and score
    val movieWithScore = movieDF.join(avgMovieScoreDF, "mid")

    // genres --> genresRDD
    val genresRDD = sparkSession.sparkContext.makeRDD(genres)

    val genresTop10MoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        // filter those movies without genre information
        case(genre, movie) => movie.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }
      .map{
        // convert to (genre, (mid, avgScore)) format
        case(genre, movie) => ( genre, (movie.getAs[Int]("mid"), movie.getAs[Double]("avg")))
      }
      .groupByKey()
      .map{
        case(genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10)
          .map(item => Recommendation(item._1, item._2)))
      }
      .toDF()

    // write into MongoDB
    storeDFInMongoDB(genresTop10MoviesDF, GENRES_TOP10_MOVIES)

    // close spark session
    sparkSession.close()

  }

  def storeDFInMongoDB(df: DataFrame, collectionName: String)(implicit mongoConfig: MongoConfig) : Unit ={
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collectionName)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }


}
