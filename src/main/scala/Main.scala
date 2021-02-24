import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("TDT4305").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val ages = Array(42, 75, 29, 64)
    val distData = sc.parallelize(ages)
    val maxAge = distData.reduce((a, b) => if (a > b) a else b)
    println(maxAge)
    spark.stop()
  }
}
