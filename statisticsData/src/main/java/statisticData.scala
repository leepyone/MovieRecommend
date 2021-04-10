import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 这个类用来完成一些统计功能
 * 1、当前人们影视
 * 2、每个电影的总评分
 *
 */
object statisticData {
    case class ratings(uid:Int, mid:Int,rating:Double,timestamp:Long )
    case class ratingAVG(mid:Int,rating:Double)

  //定义表名的常量
  val MONGODB_MOVIE = "movie"
  val MONGODB_MOVIE_DETAILS ="movieDetails"
  val MONGODB_RATINGS = "ratings"
  val MONGODB_TAG = "tags"
  //每部电影的总评分表格
  val MONGODB_RATINGS_AVG="ratingAVG"
  //统计最近时间内评分次数最多的电影
  val MONGODB_RATING_RECENTLY = "recentlyRating"
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/movieRecommender",
      "mongo.db" -> "movieRecommender",
    )
    //创建spark 环境
    val sparkConf= new SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    // 从mongodb加载数据
    val ratingDF = spark.read
      .option("uri", config("mongo.uri"))
      .option("database", config("mongo.db"))
      .option("collection", MONGODB_RATINGS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ratings]
      .toDF()
    //计算每部电影的评分平均值
    val RatingAVG = ratingDF.map(
      rating => (rating.getInt(1), rating.getDouble(2))
    )
      .rdd
      .groupByKey()
      .filter(item=>item._2.size>10)//筛选不足10条评论的电影
      .map(
        item=> ratingAVG(item._1,item._2.toList.sum/item._2.size)
      ).toDS()
    //    RatingAVG.collect().take(10).foreach(println)
    RatingAVG.write
      .option("uri",config("mongo.uri"))
      .option("database",config("mongo.db"))
      .option("collection",MONGODB_RATINGS_AVG)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.createOrReplaceTempView("ratings")
        //  近期热门统计，按照“yyyyMM”格式选取最近的评分数据，统计评分个数
        // 创建一个日期格式化工具
    val simpleDataFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate",(x:Int)=>simpleDataFormat.format(new Date(x*1000L)).toInt)

    val ratingOfYear = spark.sql("select mid ,rating,changeDate(timestamp) as yearmonth from ratings")
    ratingOfYear.createOrReplaceTempView("ratingOfYear")
//    先根据日期进行降序，然后根据 mid 数量进行降序
    val rateRecentlyMoviesDF = spark.sql("select mid,count(mid) as count,yearmonth from ratingOfYear group by yearmonth ,mid order by yearmonth desc,count desc")
    rateRecentlyMoviesDF.write
      .option("uri",config("mongo.uri"))
      .option("database",config("mongo.db"))
      .option("collection",MONGODB_RATING_RECENTLY)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()




  }
}
