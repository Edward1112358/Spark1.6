import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Edward on 2017-3-27.
  */
object MoviesRecommond {
  def main(args: Array[String]): Unit = {
    // 屏蔽日志，由于结果是打印在控制台上的，为了方便查看结果，将spark日志输出关掉
    // 解决spark日志输出的问题最好的解决办法是:修改spark日志文件，将日志写入文件中
    /// Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    /// Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("MoviesRecommond")
    val sc = new SparkContext(conf)

    case class User(userId: Int, age: Int, gender: String)
    //case class Rating(userId: Int, item: Int, rate: Double) //注意要用ALS的Rating，而不是自己定义的
    case class Ratings(userId: Int, movieId: Int, rating: Int)
    case class Movies(id: Int, moveTitle: String, releaseDate: String)

    val ratingsData = sc.textFile("file:///home/aisinobi/tmpEdward/ratings.data").map(line => line.split("::") match {
      case Array(user, item, rate, ts) => Rating(user.toInt, item.toInt, rate.toDouble)
    })
    /* val ratingsTestData = sc.textFile("file:///home/aisinobi/tmpEdward/ratingsTest.data").map(line => line.split
    ("::") match {
       case Array(user, item, rate, ts) => Ratings(user.toInt, item.toInt, rate.toDouble)
     })*/
    val usersData = sc.textFile("file:///home/aisinobi/tmpEdward/users.data").map(line => line.split("|") match {
      case Array(userId, age, gender, occupation, zipCode) => User(userId.toInt, age.toInt, gender)
    })
    val moviesData = sc.textFile("file:///home/aisinobi/tmpEdward/movies.data").map(line => line.split("::"))

    println("rate data count is: " + ratingsData.count())

    // 生成k,v形式，便于通过movieID找到movieName
    val movieDataMap = moviesData.map(x => (x(0).toInt, x(1))).collectAsMap()
    //.collect.toMap
    // 通过broadcase进行广播
    val bMovieDataMap = sc.broadcast(movieDataMap)

    val rank = 10
    val numIterations = 10
    val model = ALS.train(ratingsData, rank, numIterations, 0.001)
    val usersProducts = ratingsData.map { case Rating(user, product, rate) => (user, product) }
    ratingsData.map(x => (x.user, x.product))
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) => ((user, product), rate) }
    val ratesAndPreds = ratingsData.map { case Rating(user, product, rate) => ((user, product), rate) }.join(predictions)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    println(s"Mean squared Error = $MSE") // 均方误差

    val userID = 384
    val moviesForUser = ratingsData.keyBy(_.user).lookup(userID)
    println(s"用户$userID" + "评价过的电影:\n")
    for (movieID <- moviesForUser.map(f => f.product)) {
      println(bMovieDataMap.value.getOrElse(movieID, ""))
    }

    println(s"用户$userID" + "推荐的电影:\n")
    val recommendProds: Array[Rating] = model.recommendProducts(userID, 20)
    for (recommend <- recommendProds) {
      println(bMovieDataMap.value.getOrElse(recommend.product, ""))
    }

    println("为每个用户推荐10个电影:\n")
    val allRecommendations = model.recommendProductsForUsers(10).map {
      case (userId, recommends) =>
        val str = new StringBuilder()
        for (r <- recommends) {
          if (str.nonEmpty) {
            str.append("::")
          }
          str.append(r.product)
        }
        (userId, str.toString())
    }
    allRecommendations.take(10).foreach(println)
  }
}
