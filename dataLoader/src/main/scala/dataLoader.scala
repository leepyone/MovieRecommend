import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.http.HttpHost
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SparkSession}
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.client.indices.{CreateIndexRequest, GetIndexRequest}

/**
 * 2021年4月5日
 * 将csv文件中电影信息导入到mongodb、es中
 *
 */




object dataLoader {

  //定义几个样例类用读取并存储csv 数据

  //电影类 值存储简单的信息
  case class movie(mid:Int, title:String ,genres:String )

  // mid  名称 时长，类型， 发行日期，描述，导演，编剧，演员
  case class movieDetail(mid:Int ,title:String,timeLong:String ,genres:String,issue:String ,description:String,directors:String ,writer:String,stars:String)

  case class tag(uid:Int, mid:Int,tag:String ,timestamp:Long)

  case class ratings(uid:Int,mid:Int,rating:Double,timestamp:Long)

  //定义表名的常量
  val MONGODB_MOVIE = "movie"
  val MONGODB_MOVIE_DETAILS ="movieDetails"
  val MONGODB_RATINGS = "ratings"
  val MONGODB_TAG = "tags"

//  定义 csv文件路径
  val MOVIE_CSV = "D:\\IdeaProgram\\MovieRecommend\\dataLoader\\src\\main\\resources\\movies.csv"
  val RATINGS_CSV = "D:\\IdeaProgram\\MovieRecommend\\dataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_CSV = "D:\\IdeaProgram\\MovieRecommend\\dataLoader\\src\\main\\resources\\tags.csv"
  val MOVIE_DETAILS_CSV = "D:\\IdeaProgram\\MovieRecommend\\dataLoader\\src\\main\\resources\\detail.csv"


  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/movieRecommender",
      "mongo.db" -> "movieRecommender",
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommend",
      "es.cluster.name" -> "elasticsearch"
    )
    //创建spark 环境
    val sparkConf= new SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val movieRDD = spark.sparkContext.textFile(MOVIE_CSV)
    val movieDS = movieRDD.map(
      item => {
        val attrs = item.split(",")
        movie(attrs(0).toInt, attrs(1).trim, attrs(2).trim)
      }
    ).toDS()
//    movieDS.collect().take(10).foreach(println)

    val movieDetailsDS = spark.read.format("csv")
      .schema(Encoders.product[movieDetail].schema)
      .load(MOVIE_DETAILS_CSV).as[movieDetail]
//    movieDetailsDS.take(10).foreach(println)

    val ratingsRDD = spark.sparkContext.textFile(RATINGS_CSV)
    val ratingsDS = ratingsRDD.map(
      item => {
        val attrs = item.split(",")
        ratings(attrs(0).toInt, attrs(1).toInt, attrs(2).toDouble,attrs(3).toLong)
      }
    ).toDS()
//    ratingsDS.take(10).foreach(println)

    val tagsRDD = spark.sparkContext.textFile(TAG_CSV)
    val tagsDS = tagsRDD.map(
      item => {
        val attrs = item.split(",")
        tag(attrs(0).toInt, attrs(1).toInt, attrs(2).trim,attrs(3).toLong)
      }
    ).toDS()
//    tagsDS.take(10).foreach(println)

    //从csv文件中读取到数据后江其写入到mongoDB和es中

//    写入mongoDB
val mongoClient: MongoClient = MongoClient(MongoClientURI(config("mongo.uri")))
//    如果存在collection就删除
        mongoClient(config("mongo.db"))(MONGODB_MOVIE).dropCollection()
        mongoClient(config("mongo.db"))(MONGODB_RATINGS).dropCollection()
        mongoClient(config("mongo.db"))(MONGODB_TAG).dropCollection()
        mongoClient(config("mongo.db"))(MONGODB_MOVIE_DETAILS).dropCollection()
    //写入
    movieDS.write
      .option("uri",config("mongo.uri"))
      .option("database",config("mongo.db"))
      .option("collection",MONGODB_MOVIE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    movieDetailsDS.write
      .option("uri",config("mongo.uri"))
      .option("database",config("mongo.db"))
      .option("collection",MONGODB_MOVIE_DETAILS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingsDS.write
      .option("uri",config("mongo.uri"))
      .option("database",config("mongo.db"))
      .option("collection",MONGODB_RATINGS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    tagsDS.write
      .option("uri",config("mongo.uri"))
      .option("database",config("mongo.db"))
      .option("collection",MONGODB_TAG)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //将 details 存入 es中
    val esClient = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")))

    if(esClient.indices().exists(new GetIndexRequest(config("es.index")),RequestOptions.DEFAULT)){
      esClient.indices().delete(new DeleteIndexRequest(config("es.index")),RequestOptions.DEFAULT)
    }
    esClient.indices().create(new CreateIndexRequest(config("es.index")),RequestOptions.DEFAULT)

    esClient.close()
    movieDetailsDS.write
      .option("es.nodes", config("es.httpHosts"))
      .option("es.http.timeout", "100m")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(config("es.index")+"/"+"movies")

  }

}
