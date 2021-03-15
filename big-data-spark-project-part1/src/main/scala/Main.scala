import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import java.util.Base64
import java.time

object Main {
  // Loader for task 1: removing headers and making a column(array) based RDD
  def LoadCsvToRDD(filePath: String, sparkContext: SparkContext): RDD[Array[String]] = {
    val RDD = sparkContext.textFile(filePath)
    val removedHeader = RDD.mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
    removedHeader.map(string => string.split('\t'))
  }

  // Function to convert strings to int, and NULL to -1
  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => -1
    }
  }

  // Convert string of date to LocalDateTime
  def StringToDateTime(dateTimeString: String): Option[time.LocalDateTime] = {
    if (dateTimeString.equals("NULL")){
      return None
    }
    val dateTimeArray= dateTimeString.split(" ")
    val dateArray = dateTimeArray(0).split("-")
    val timeArray = dateTimeArray(1).split(":")
    Some(time.LocalDateTime.of(dateArray(0).toInt, dateArray(1).toInt, dateArray(2).toInt, timeArray(0).toInt,
      timeArray(1).toInt, timeArray(2).toInt))
  }

  // Pearson Correlation for task 2.5
  def PearsonCorrelationCoefficient(x: Array[Int], y: Array[Int]): Double = {
    val avgX = x.sum.toFloat/x.length
    val avgY = y.sum.toFloat/y.length
    val newX = x.map(x => x-avgX)
    val newY = y.map(y => y-avgY)
    val sumXY = newX.zip(newY).map(t => t._1*t._2).sum
    val sumXSquared = newX.map(x => x*x).sum
    val sumYSquared = newY.map(y => y*y).sum

    sumXY/Math.sqrt(sumXSquared*sumYSquared)
  }

  // Entropy calculation for task 2.6
  def Entropy(array: Array[Int]): Double ={
    val len = array.sum
    val log2 = (x: Double) => Math.log10(x)/Math.log10(2.0)
    val p = (x: Int) => x/len.toFloat

    -array.map(x => p(x)*log2(p(x))).sum

  }


  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("TDT4305").setMaster("local[*]")
    //    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.appName("TDT4305").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    var dataPath = "./data/"
    var writePath = "./writedata/"

    if (args.length > 0){
      dataPath = args(0)
      writePath = args(0)
    }
    // Task 1: load RDDs and convert to correct data type as well as decoding base64 encodings

    // Id, PostTypeId, CreationDate, Score, ViewCount, Body, OwnerUserId, LastActivityDate, Title, Tags, AnswerCount,
    // CommentCount, FavoriteCount, CloseDate
    val posts = LoadCsvToRDD(dataPath + "posts.csv", sc).map(row => (toInt(row(0)), toInt(row(1)),
      StringToDateTime(row(2)),toInt(row(3)), toInt(row(4)), new String(Base64.getDecoder.decode(row(5))),
      toInt(row(6)), StringToDateTime(row(7)), row(9), row(10), toInt(row(11)), toInt(row(12)),
      StringToDateTime(row(13))))
    // PostId, Score, Text, CreationDate, UserId
    val comments = LoadCsvToRDD(dataPath + "comments.csv", sc).map(row => (toInt(row(0)), toInt(row(1)),
      new String(Base64.getDecoder.decode(row(2))), StringToDateTime(row(3)), toInt(row(4))))
    // Id, Reputation, CreationDate, DisplayName, LastAccessDate, AboutMe, Views, UpVotes, DownVotes
    val users = LoadCsvToRDD(dataPath + "users.csv", sc).map(row => (toInt(row(0)),
      toInt(row(1)), StringToDateTime(row(2)), row(3), row(4), row(5), toInt(row(6)), toInt(row(7)), toInt(row(8))))
    // UserId, Name, Date, Class
    val badges = LoadCsvToRDD(dataPath + "badges.csv", sc).map(row => (toInt(row(0)), row(1),
      StringToDateTime(row(2)), row(3)))




    // Task 2
    // 2.1 Average lengths

    // get length of comments
    val averageComments = comments.map(row => row._3.length).reduce((x, y) => (x + y)/2)
    // get length of answers
    val answers = posts.filter(row => row._2 == 2)
    val averageAnswers = answers.map(row => row._6.length).reduce((x, y) => (x + y)/2)

    // get length of questions
    val questions = posts.filter(row => row._2 == 1)
    val averageQuestions = questions.map(row => row._6.length).reduce((x, y) => (x + y)/2)

    // 2.2 First and last questions
    val firstQuestion = questions.reduce((row1, row2) => {
      val row1DateTime = row1._3.get
      val row2DateTime = row2._3.get

      if (row1DateTime.isBefore(row2DateTime)){
        row1
      } else{
        row2
      }
    })

    val firstQuestionUser = users.filter(row => row._1 == firstQuestion._7)

    val lastQuestion = questions.reduce((row1, row2) => {
      val row1DateTime = row1._3.get
      val row2DateTime = row2._3.get

      if (row1DateTime.isAfter(row2DateTime)){
        row1
      } else{
        row2
      }
    })

    val lastQuestionUser = users.filter(row => row._1 == lastQuestion._7)

    // 2.3 User with most questions and user with the most answers
    val usersID = users.filter(row => row._1 != -1).map(row => (row._1, row))

    val validQuestions = questions.filter(row => row._7 != -1).map(row => (row._7, row))
    val topUsersQuestions = usersID.join(validQuestions).map(row => (row._1, 1)).reduceByKey(_ + _).top(1)(ord = Ordering.by(_._2))

    val validAnswers = answers.filter(row => row._7 != -1).map(row => (row._7, row))
    val topUsersAnswers = usersID.join(validAnswers).map(row => (row._1, 1)).reduceByKey(_ + _).top(1)(ord = Ordering.by(_._2))


    // 2.4 Number of users with < 3 badges
    val badgeTask = badges.map(row => (row._1, 1)).reduceByKey(_ + _).filter(row => row._2 < 3).count()

    // 2.5 Pearson correlation between #upvotes and #downvotes
    val upVotes = users.map(row => row._8).collect()
    val downVotes = users.map(row => row._9).collect()
    val pearsonCoefficient = PearsonCorrelationCoefficient(upVotes, downVotes)

    // 2.6 Entropy of userIDS with >= 1 comments
    val commentCountByUser = comments.map(row => (row._5, 1)).reduceByKey(_ + _).filter(row => row._2 >= 1).map(row => row._2).collect()
    val entropy = Entropy(commentCountByUser)


    // 3.1 Graph of comments and posts users
    val simpleComments = comments.map(row => (row._1, row._5))
    val simplePosts = posts.map(row => (row._1, row._7))
    // (comment userID, post userID, weight) also includes comments on own posts
    val commentsPosts = simpleComments.join(simplePosts).map(row => ((row._2._1, row._2._2), 1)).reduceByKey(_ + _).map(row => (row._1._1, row._1._2, row._2))

    // 3.2 Create Dataframe
    val columns = Seq("src", "dest", "weight")
    val commentsDF = spark.createDataFrame(commentsPosts).toDF(columns:_*)

    // 3.3 UserIDs with most comments
    val topUsersComments = commentsDF.groupBy("src").agg(sum("weight")).orderBy(desc("sum(weight)"))

    //3.4 User displayname with most received comments
    val topReceivedComments = commentsDF.groupBy("dest").agg(sum("weight")).orderBy(desc("sum(weight)"))

    val usersDisplayName = spark.createDataFrame(users.map(row => (row._1, row._4))).toDF("userID", "displayName")
    val topReceivedCommentsName = topReceivedComments.alias("a").join(usersDisplayName.alias("b"),
      topReceivedComments("dest") === usersDisplayName("userID"),"inner")
      .orderBy(desc("sum(weight)")).select("b.userID", "b.displayName","a.sum(weight)")

    // 3.5 save DataFrame as CSV
    topReceivedCommentsName.coalesce(1).write.option("header", "true").option("sep", "\t").mode("append").csv(writePath)

    // Print all answers
    // Answers to task 1:
    println("Task 1:")
    println(s"The length of the posts RDD is ${posts.count()}")
    println(s"The length of the comments RDD is ${comments.count()}")
    println(s"The length of the users RDD is ${users.count()}")
    println(s"The length of the badges RDD is ${badges.count()}")
    println()

    //Answers to task 2:
    println("Task 2:")
    // 2.1
    println(s"Average length of questions $averageComments")
    println(s"Average length of answers $averageAnswers")
    println(s"Average length of comments $averageComments")
    println()

    // 2.2
    println(s"First question by user ${firstQuestionUser.collect()(0)._4} at date ${firstQuestion._3.get}")
    println(s"Last question by user ${lastQuestionUser.collect()(0)._4} at date ${lastQuestion._3.get}")
    println()

    // 2.3
    println(s"User ID with the most questions ${topUsersQuestions(0)._1} with ${topUsersQuestions(0)._2} questions")
    println(s"User ID with the most answers ${topUsersAnswers(0)._1} with ${topUsersAnswers(0)._2} answers")
    println()

    // 2.4
    println(s"# of users with less than 3 badges: $badgeTask")
    println()

    // 2.5
    println(s"Pearson correlation coefficient between upvotes and downvotes: $pearsonCoefficient")
    println()

    // 2.6
    println(s"Entropy of users with at least one comment $entropy")
    println()

    // Task 3
    println("Task 3:")
    // 3.3
    topUsersComments.show(10)
    // 3.4
    topReceivedCommentsName.show(10)

    //spark.stop()
  }
}
