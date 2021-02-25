import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
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

  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => -1
    }
  }


  def AverageLengthText(tableRDD: RDD[Array[String]], indexOfText: Int): Int = {
    val textLength = tableRDD.map(row => row(indexOfText).length)
    textLength.reduce((x, y) => (x + y)/2)
  }

  def StringToDateTime(dateTimeString: String): Option[time.LocalDateTime] = {
    if (dateTimeString.equals("NULL")){
      return None
    }
    val dateTimeArray= dateTimeString.split(" ")
    val dateArray = dateTimeArray(0).split("-")
    val timeArray = dateTimeArray(1).split(":")
    Some(time.LocalDateTime.of(dateArray(0).toInt, dateArray(1).toInt, dateArray(2).toInt, timeArray(0).toInt, timeArray(1).toInt, timeArray(2).toInt))
  }


  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("TDT4305").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    // Task 1: load RDDs
    val dataPath = "./data/"
    val posts = LoadCsvToRDD(dataPath + "posts.csv", sc).map(row => (toInt(row(0)), toInt(row(1)), StringToDateTime(row(2)),toInt(row(3)), toInt(row(4)), new String(Base64.getDecoder.decode(row(5))), toInt(row(6)), StringToDateTime(row(7)), row(9), row(10), toInt(row(11)), toInt(row(12)), StringToDateTime(row(13))))
    val comments = LoadCsvToRDD(dataPath + "comments.csv", sc).map(row => (toInt(row(0)), toInt(row(1)), new String(Base64.getDecoder.decode(row(2))), StringToDateTime(row(3)), toInt(row(4))))
    val users = LoadCsvToRDD(dataPath + "users.csv", sc).map(row => (toInt(row(0)), toInt(row(1)), StringToDateTime(row(2)), row(3), row(4), row(5), toInt(row(6)), toInt(row(7)), toInt(row(8))))
    val badges = LoadCsvToRDD(dataPath + "badges.csv", sc).map(row => (toInt(row(0)), row(1), StringToDateTime(row(2)), row(3)))


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

    // 2.3 Users with most questions and user with the most answers
    val usersID = users.filter(row => row._1 != -1).map(row => (row._1, row))
    usersID.take(1).foreach(println)
    val validQuestions = questions.filter(row => row._7 != -1).map(row => (row._7, row))
    validQuestions.take(1).foreach(println)
    val usersWithQuestions = usersID.join(validQuestions).map(row => (row._1, 0)).groupByKey()
    println(usersWithQuestions.take(5)(1)._2)

    val validAnswers = answers.filter(row => row._7 != -1).map(row => (row._7, row))

    // Print all answers
    // Answers to task 1:
    println(s"The length of the posts RDD is ${posts.count()}")
    println(s"The length of the comments RDD is ${comments.count()}")
    println(s"The length of the users RDD is ${users.count()}")
    println(s"The length of the badges RDD is ${badges.count()}")

    //Answers to task 2:
    println(averageComments)
    println(averageAnswers)
    println(averageQuestions)

    // 2.2
    println(s"First question by user ${firstQuestionUser.collect()(0)._4} at date ${firstQuestion._3.get}")
    println(s"Last question by user ${lastQuestionUser.collect()(0)._4} at date ${lastQuestion._3.get}")

    // 2.3

    spark.stop()
  }
}
