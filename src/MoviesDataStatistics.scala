import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Edward on 2017-3-20.
  */
object MoviesDataStatistics {

  //userId,movieId,rating,timestamp
  //1,31,2.5,1260759144
  case class Ratings(userId: Int, movieId: Int, rating: Double)

  //movieId,title,genres
  //1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
  case class Movies(movieId: Int, movieTitle: String, genres: String)

  case class Users(id: Int, age: Int, gender: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("MovieRecommondNew")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val rdd = sc.textFile("file:///home/aisinobi/tmpEdward/ratings.csv").map(line => line.split(",")).map(line =>
      Ratings(line(0).toInt, line(1).toInt, line(2).toDouble))
    val ratingDF: DataFrame = rdd.toDF()
    ratingDF.registerTempTable("ratings")

    val movieDF = sc.textFile("file:///home/aisinobi/tmpEdward/movies.csv").map(line => line.split(",")).map(line =>
      Movies(line(0).toInt, line(1), line(2))).toDF()
    movieDF.registerTempTable("movies")

    val userDF = sc.textFile("file:///home/aisinobi/tmpEdward/users.csv").map(line => line.split(",")).map(line =>
      Users(line(0).toInt, line(1).toInt, line(2))).toDF()
    userDF.registerTempTable("users")

    //两种方式查看用户ID为338的评分记录
    sqlContext.sql("select * from ratings where userId = 338").show() //SQL
    ratingDF.filter(ratingDF("userId").equalTo(338)).show //DataFrame

    sqlContext.sql("select * from movies where movieId = 1").show()
    movieDF.filter(movieDF("movieId").equalTo(1)).show()

    //a: Dataframe
    //b: RDD[String]
    val a = sqlContext.sql("select * from ratings where userId = 338")
    val b = a.map(x => x + "#")


    sqlContext.sql("select r.userId, m.movieTitle, r.rating from movies m " +
      "inner join ratings r on m.movieId = r.movieId and r.userId = 338 order by r.rating desc ").show()
    //我写的，后进行filter，效率低
    movieDF.join(ratingDF, movieDF("movieId") === ratingDF("movieId"), "inner").filter(ratingDF("userId") === 338)
      .orderBy(ratingDF("rating").desc).show()
    //文章中先进行filter
    val resultDF = movieDF.join(ratingDF.filter(ratingDF("userId").equalTo(338)), movieDF("movieId").equalTo(ratingDF
    ("movieId"))).sort(ratingDF("rating").desc).select("userId", "movieTitle", "rating")
    resultDF.collect().foreach(println)


    val saveOptions = Map("header" -> "true", "path" -> "file:///home/aisinobi/tmpEdward/rat_movie.csv")
    // 需要相应jar包
    // resultDF.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
    resultDF.write.format("json").mode(SaveMode.Overwrite).options(saveOptions).save()


    // 评论电影最多的用户id
    sqlContext.sql("select userId, count(*) as count from ratings group by userId order by count desc").show(3)
    ratingDF.groupBy("userId").count().select("userId", "count").sort($"count".desc).show

    // 被用户评论最多的电影id
    sqlContext.sql("select movieId, count(*) as count from ratings group by movieId order by count desc").show(3)

    // 被用户评论最多的电影的详细信息
    // 比较有难度！
    // 我写的
    sqlContext.sql("SELECT r.movieId, MAX(m.movieTitle) movieTitle, MAX(m.genres) genres, COUNT(*) count FROM movies " +
      "m, ratings r WHERE m.movieId" +
      " = r.movieId GROUP BY r.movieId order by count(*) desc").show
    // 文章中
    val movieIDGroupDF = ratingDF.groupBy("movieId").count()
    val movieCountDF = movieIDGroupDF.join(movieIDGroupDF.agg(max($"count").alias("maxCount"))).filter($"count"
      .equalTo($"maxCount")) //agg(Map("count"->"max"))
    movieCountDF.join(movieDF).filter(movieCountDF("movieId").equalTo(movieDF("movieId"))).select(movieDF("movieId"),
      movieDF("movieTitle")).show()
    movieCountDF.as("a").join(movieDF.as("b")).filter(($"a.movieId").equalTo($"b.movieId")).select($"a.movieId", $"b" +
      $".movieTitle").show()


    // 评论电影年龄最大/最小者的详细信息
    sqlContext.sql("select uu.id, uu.age from users uu, (select max(u.age) maxAge, min(u.age) minAge from ratings" +
      " r, users u where r.userId = u.id) t where uu.age in (t.maxAge, t.minAge)").show
    // 我写的
    userDF.as("u").join(ratingDF.as("r"), $"u.id".equalTo($"r.userId"))
      .select(max($"u.age").as("maxAge"), min($"u.age").as("minAge"))
      .join(userDF.as("uu"), $"uu.age".isin($"maxAge", $"minAge"))
      .select("id", "age", "gender").show
    // 文章中
    ratingDF.join(userDF, ratingDF("userId").equalTo(userDF("id")))
      .agg(min($"age").alias("min_age"), max($"age").alias("max_age"))
      .join(userDF, $"age".isin($"min_age", $"max_age"))
      .select("id", "age", "gender").show(2)


    // 25至30岁的用户欢迎的电影
    sqlContext.sql("select u.age, avg(r.rating) from ratings r, users u where r.userId = u.id and u.age between 25 and 30 group by u.age order by avg(r.rating) desc").show
    // 我写的
    ratingDF.as("r").join(userDF.as("u").filter($"u.age".between(25,30)), $"r.userId".equalTo($"u.id")).groupBy($"u.age").agg(avg("r.rating").as("avgRating")).sort($"avgRating".desc).show
  }
}
