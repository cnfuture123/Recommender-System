package cn.online

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class MongoConfig(uri:String, db:String)

// 定义一个基准推荐对象
case class Recommendation( mid: Int, score: Double )

// 定义基于预测评分的用户推荐列表
case class UserRecs( uid: Int, recs: Seq[Recommendation] )

// 定义基于LFM电影特征向量的电影相似度列表
case class MovieRecs( mid: Int, recs: Seq[Recommendation] )

// 定义连接助手对象，序列化
object ConnHelper extends Serializable {
  lazy val jedis = new Jedis("master", 6379)
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://master:27017/recommender"))
}

object OnlineRecommender {

  val MAX_USER_RATINGS_NUM = 10
  val MAX_SIM_MOVIES_NUM = 10
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"
  val MONGODB_ONLINE_RECS_COLLECTION = "OnlineRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://master:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // get streaming context
    val sc = sparkSession.sparkContext
    val streamingContext = new StreamingContext(sc, Seconds(2))

    import sparkSession.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // get similar movies
    val simMovieMatrix = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{
        // convert to map format for better query performance
        movieRecs => (movieRecs.mid, movieRecs.recs.map(x => (x.mid, x.score)).toMap)
      }.collectAsMap()

    val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)

    // kafka connection config
    val kafkaParam = Map(
      "bootstrap.servers" -> "192.168.66.132:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    // Kafka stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
    )

    // 把原始数据UID|MID|SCORE|TIMESTAMP 转换成评分流
    val ratingStream = kafkaStream.map{
      msg =>
        val fields = msg.value().split("\\|")
        (fields(0).toInt, fields(1).toInt, fields(2).toDouble, fields(3).toLong)
    }

    // 核心实时算法部分
    ratingStream.foreachRDD{
      rdds => rdds.foreach{
        case (uid, mid, score, timestamp) => {
          println("processing incoming rating data")

          // 1. 从redis里获取当前用户最近的K次评分，保存成Array[(mid, score)]
          val userRecentRatings = getUserRecentRatings(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

          // 2. 从相似度矩阵中取出当前电影最相似的N个电影，作为备选列表，Array[mid]
          val candidateMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

          // 3. 对每个备选电影，计算推荐优先级，得到当前用户的实时推荐列表，Array[(mid, score)]
          val onlineRecs = computeOnlineRecs(candidateMovies, userRecentRatings, simMovieMatrixBroadCast.value)

          // 4. 把推荐数据保存到MongoDB
          saveDataToMongoDB(uid, onlineRecs)

        }
      }
    }

    // start to receive data
    streamingContext.start()

    println("streaming data started")

    streamingContext.awaitTermination()

  }

  // Redis操作返回的是java类，为了用map操作需要引入转换类
  import scala.collection.JavaConversions._

  def getUserRecentRatings(num: Int, uid: Int, jedis: Jedis) : Array[(Int, Double)] = {
    // 从redis读取数据，用户评分数据保存在 uid:UID 为key的队列里，value是 MID:SCORE
    jedis.lrange("uid:" + uid, 0, num - 1).map{
      item =>
        val fields = item.split("\\:")
        (fields(0).trim.toInt, fields(1).trim.toDouble)
    }.toArray
  }

  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                     (implicit mongoConfig: MongoConfig): Array[Int] = {

    // 1. 从相似度矩阵中拿到所有相似的电影
    val allSimMovies = simMovies(mid).toArray

    // 2. 从MongoDB中查询用户已看过的电影
    val ratedMovies = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map{
        item => item.get("mid").toString.toInt
      }

    // 3. 过滤评分过的电影
    allSimMovies.filter(x => ! ratedMovies.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)
  }

  def computeOnlineRecs(candidateMovies: Array[Int], userRecentRatings: Array[(Int, Double)],
                        simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] = {
    // 定义一个ArrayBuffer，用于保存每一个备选电影的基础得分
    val scores = ArrayBuffer[(Int, Double)]()

    // 定义一个HashMap，保存每一个备选电影的增强减弱因子
    val increMap = mutable.HashMap[Int, Int]()
    val decreMap = mutable.HashMap[Int, Int]()

    for (candidateMovie <- candidateMovies; userRecentRating <- userRecentRatings) {
      // 计算备选电影和最近评分电影的相似度
      val simScore = getMovieSimScore(candidateMovie, userRecentRating._1, simMovies)

      if (simScore > 0.7) {
        // 计算备选电影的基础推荐得分
        scores += ((candidateMovie, simScore * userRecentRating._2))
        if(userRecentRating._2 > 3.0){
          increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
        } else {
          decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
        }
      }
    }

    // 根据备选电影的mid做groupby，根据公式去求最后的推荐评分
    scores.groupBy(_._1).map{
      case (mid, scoreList) =>
        (mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid, 1))
          - log(decreMap.getOrDefault(mid, 1)))
    }.toArray.sortWith(_._2 > _._2)
  }

  def getMovieSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double = {
    simMovies.get(mid1) match {
      case Some(movies) => movies.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  // 求一个数的对数，利用换底公式，底数默认为10
  def log(m: Int): Double ={
    val N = 10
    math.log(m)/ math.log(N)
  }

  def saveDataToMongoDB(uid: Int, onlineRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    val onlineRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_ONLINE_RECS_COLLECTION)
    // 如果表中已有uid对应的数据，则删除
    onlineRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    // insert data
    onlineRecsCollection.insert(
      MongoDBObject(
        "uid" -> uid,
        "recs" -> onlineRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))
    ))
  }

}
