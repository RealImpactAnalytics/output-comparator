import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

abstract class TestResults
case class Ok extends TestResults
case class Error(id : String, trustedValue : String, testValue : String, msg : String) extends TestResults
abstract class Missing(columnName : String) extends TestResults
case class TrustedMissing(columnName : String) extends Missing(columnName)
case class TestMissing(columnName : String) extends Missing(columnName)

object ResultsTest {

	val separator = ","

	def main(args: Array[String]){
		/*
		 * args: test-data-header test-data data-header data
		 */

		val conf = new SparkConf().setAppName("Results test")
		val sc = new SparkContext(conf)


		// Consider the first column as the key
		def columnSplit(line : String) : (String, Array[String]) = {
		  var s = line.split(separator)
			(s.head, s.tail)
		}

		// Get the data 
		// For the header the first entries is the id which we don't verify
		val testDataHeader = sc.textFile(args(0)).first.split(separator).tail
		val trustedDataHeader = sc.textFile(args(2)).first.split(separator).tail
		val testData = sc.textFile(args(1)).map(columnSplit(_))
		val trustedData = sc.textFile(args(3)).map(columnSplit(_))

		// Compute the missing column
		val missingTrustedColumn = columnDiff(trustedDataHeader, testDataHeader)
		val missingTestColumn = columnDiff(testDataHeader, trustedDataHeader)

		println("Trusted missing column :")
		missingTrustedColumn.foreach(i => println(trustedDataHeader(i)))
		println("Test missing column :")
		missingTestColumn.foreach(i => println(testDataHeader(i)))

		// Compute the column mapping
		val columnMapping = columnMap(testDataHeader, trustedDataHeader)

		/*
		 * Verify if 2 rows correspond
		 * return an array whith ok if 2 entries correspond and an error if not
		 */
		def verifyRow(id : String, test : Array[String], trusted : Array[String]) : Array[TestResults] = {
				columnMapping.map{
					case (testIndex, trustedIndex) =>
						if(test(testIndex) == trusted(trustedIndex)){
							Ok()
						}else{
							val testValue = test(testIndex) 
							val trustedValue = trusted(trustedIndex)
							Error(id, trustedValue, testValue,
								s"column : ${testDataHeader(testIndex)} id : ${id} test : ${testValue} instead of ${trustedValue}")
						}
				}.toArray
		}

		// Compute the errors
		val results = testData.join(trustedData).map {
			case (id, (test, trusted)) => verifyRow(id, test, trusted)
		}

		println("results : ")
		// Print some results
		results.take(10).foreach(_.zipWithIndex.foreach{
			case (Ok(), i) => println(s"column : ${testDataHeader(i)} OK")
			case (Error(id, trustedValue, testValue, msg), i) => println(msg)
		})
	}

	/*
	 * Return a map a indice -> b indice
	 */
	def columnMap(a : Array[String], b : Array[String]) : Map[Int,Int] = {
		a.zipWithIndex.map{ case (h, i) => (i, b.indexOf(h))}.toMap.filter{ case (_, bi) => bi != -1}
	}

	/* 
	 * Return a array which contains the index of the column that are in a but not in b
	 */ 
	def columnDiff(a : Array[String], b : Array[String]) : Array[Int]= {
		for ((h, i) <- a.zipWithIndex if !b.contains(h)) yield {
			i
		}
	}

}
