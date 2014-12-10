// $../bin/spark-submit --class "Q1" --master local[4] target/scala-2.10.4/simple-project_2.10.4-1.1.jar 94115
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Q1 {
  def main(args: Array[String]) {
    val textFile = "hdfs://localhost:9000/q1/users.dat"
    val zipCode = if (args.length>0) args(0) else 94115
    val zipCodeString = zipCode.toString
    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(textFile)
    val lines = logData.filter(line => line.contains(zipCodeString)).map(_.split("\\::")(0))
    lines.foreach(println)
  }
}
