import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Spark{
		val conf = new SparkConf().setAppName("Results test")
		val sc = new SparkContext(conf)
}
