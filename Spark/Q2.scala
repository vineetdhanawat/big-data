import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Q2 {
  def main(args: Array[String]) {
    val textFile = "hdfs://localhost:9000/q1/ratings.dat" // Should be some file on your system
    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(textFile)
    val movieRatings = sc.textFile(textFile).map { line =>
      val fields = line.split("\\::")
      (fields(1).toInt, fields(2).toDouble)
    }

    val movieAvgRatings = movieRatings.groupByKey().map(data => { val avg = data._2.sum / data._2.size; (avg,data._1)})
    val result = movieAvgRatings.sortByKey(false,1).take(10)
    result.foreach{
		case(key,value) =>
			println(value,key)
	}
  }
}
