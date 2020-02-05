package cn.dao

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

// 定义样例类
// Movie class
case class Movie(mid: Int, name: String, desc: String, timelong: String, issue: String, shoot: String,
                 language: String, genres: String, actors: String, directors: String)

// Rating class
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Long)

// Tag class
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Long)

/**
 * MongoDB Config
 *
 * @param uri MongoDB connection
 * @param db  MongoDB database
 */
case class MongoConfig(uri: String, db: String)

/**
 * ElasticSearch Config
 *
 * @param httpHosts      http主机列表，逗号分隔
 * @param transportHosts transport主机列表
 * @param index          需要操作的索引
 * @param clusterName    集群名称，默认elasticsearch
 */
case class ESConfig(httpHosts: String, httpPort: String, transportHosts: String,
                    transportPort: String, index: String, clusterName: String)


object DataLoader {

  val MOVIE_DATA_PATH = "D:\\WorkSpace\\MovieRecommenderWorkSpace\\Recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\WorkSpace\\MovieRecommenderWorkSpace\\Recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\WorkSpace\\MovieRecommenderWorkSpace\\Recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"


  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://master:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "master",
      "es.httpPort" -> "9200",
      "es.transportHosts" -> "master",
      "es.transportPort" -> "9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"
    )

    // SparkConf
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))
    // SparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 对DataFrame和Dataset进行操作许多操作都需要这个包进行支持
    import sparkSession.implicits._

    // preprocess data
    val movieRDD = sparkSession.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF = movieRDD.map(item => {
      val attr = item.split("\\^")
      Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
        attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
    }).toDF()

    val ratingRDD = sparkSession.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toLong)
    }).toDF()

    val tagRDD = sparkSession.sparkContext.textFile(TAG_DATA_PATH)
    val tagDF = tagRDD.map(item => {
      val attr = item.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toLong)
    }).toDF()

    // 声明一个隐式的配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 将数据保存到 MongoDB 中
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    import org.apache.spark.sql.functions._

    /**
     * mid, tags
     * new tags: tag1|tag2|tag3
     */
    val newTag = tagDF
      .groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")

    // join movie data and newTag
    val movieWithTagsDF = movieDF.join(newTag, Seq("mid", "mid"), "left")

    implicit val esConfig = ESConfig(
      config("es.httpHosts"),
      config("es.httpPort"),
      config("es.transportHosts"),
      config("es.transportPort"),
      config("es.index"),
      config("es.cluster.name")
    )

    // store data in ES
    storeDataInES(movieWithTagsDF)

    // close spark session
    sparkSession.close()

  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)
                        (implicit mongoConfig: MongoConfig): Unit = {
    // 新建一个MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //如果 MongoDB 中有对应的数据库，那么应该删除
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //将当前数据写入到 MongoDB
    movieDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDF
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    // close DB connection
    mongoClient.close()
  }

  def storeDataInES(movieDF: DataFrame)(implicit esConfig: ESConfig): Unit = {
    // Initialization
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clusterName).build()

    // Create client
    val esClient = new PreBuiltTransportClient(settings).addTransportAddress(
      new InetSocketTransportAddress(InetAddress.getByName(esConfig.transportHosts), esConfig.transportPort.toInt))

    // Clear existed index if any
    if(esClient.admin().indices().exists(new
        IndicesExistsRequest(esConfig.index)).actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }

    // Create new index
    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

    // Write data to ES
    movieDF
      .write
      .option("es.nodes", esConfig.httpHosts + ":" + esConfig.httpPort)
      .option("es.http.timeout", "1m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + ES_MOVIE_INDEX)


  }


}
