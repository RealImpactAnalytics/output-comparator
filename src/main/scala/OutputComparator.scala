import Spark.sc
import org.apache.spark.SparkContext._

abstract class TestResults
case object Ok extends TestResults
case class Error(id : String, trustedValue : String, testValue : String, msg : String) extends TestResults
abstract class Missing(columnName : String) extends TestResults
case class TrustedMissing(columnName : String) extends Missing(columnName)
case class TestMissing(columnName : String) extends Missing(columnName)

class OutputComparator(val testDataHeader : Array[String],
	val trustedDataHeader : Array[String],
	val testData : org.apache.spark.rdd.RDD[(String, Array[String])],
	val trustedData : org.apache.spark.rdd.RDD[(String, Array[String])])
	extends java.io.Serializable {

	val N_ERROR = 10 // maximum number of errors by column for the human readable report

	// Compute the missing column
	val missingTrustedColumn = columnDiff(trustedDataHeader, testDataHeader)
	val missingTestColumn = columnDiff(testDataHeader, trustedDataHeader)

	// Compute the column mapping
	val columnMapping = columnMap(testDataHeader, trustedDataHeader)

	// Compute the errors
	val results = testData.join(trustedData).map {
		case (id, (test, trusted)) => verifyRow(id, test, trusted)
	}

	// Aggregate the errors, each element of the array is the list of error for a column
	// The list contain either only Ok or a N_ERROR errors
	val synthethicTestResults = results.reduce{ (A, B) =>
		A.zip(B).map{
			case (Ok :: Nil, Ok :: Nil) => Ok :: Nil
			case (error, Ok :: Nil) => error
			case (Ok :: Nil, error) => error
			case (errorA, errorB) => (errorA ::: errorB).take(N_ERROR)
		}
	}

	/*
	 * Verify if 2 rows correspond
	 * return an array of list whith ok if 2 entries correspond and an error if not
	 * We use list so we can aggregate it in a list of error easily after
	 */
	def verifyRow(id : String, test : Array[String], trusted : Array[String]) : Array[List[TestResults]] = {
			columnMapping.map{
				case (testIndex, trustedIndex) =>
					if(test(testIndex) == trusted(trustedIndex)){
						List(Ok)
					}else{
						val testValue = test(testIndex) 
						val trustedValue = trusted(trustedIndex)
						List(Error(id, trustedValue, testValue,
							s"column : ${testDataHeader(testIndex)} id : ${id} test : ${testValue} instead of ${trustedValue}"))
					}
			}.toArray
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


	// Print the error report
	columnMapping.zip(synthethicTestResults).foreach{case ((trustedColumnIndex, _), errors) =>
		val columnName = trustedDataHeader(trustedColumnIndex)
		errors match {
			case (Ok :: Nil) => println(s"column : $columnName is OK")
			case errors => errors.foreach{
				case e : Error => println(s"column : $columnName error : for id : ${e.id} the value is ${e.testValue} but it should be ${e.trustedValue}")
			}
		}
	}

	println("Trusted missing column :")
	missingTrustedColumn.foreach(i => println(trustedDataHeader(i)))
	println("Test missing column :")
	missingTestColumn.foreach(i => println(testDataHeader(i)))

	println("results : ")
	// Print some results
	results.take(10).foreach(_.zipWithIndex.foreach{
		case (List(Ok), i) => println(s"column : ${testDataHeader(i)} OK")
		case (List(Error(id, trustedValue, testValue, msg)), i) => println(msg)
	})

}

object OutputComparator{

	val separator = "," // for the csv files

	// Consider the first column as the key
	def columnSplit(line : String) : (String, Array[String]) = {
		var s = line.split(separator)
		(s.head, s.tail)
	}

	def getHeader(headerPath : String) : Array[String]={
		sc.textFile(headerPath).first.split(separator).tail
	}

	def getData(dataPath : String) : org.apache.spark.rdd.RDD[(String, Array[String])] = {
		sc.textFile(dataPath).map(columnSplit(_))
	}


	def apply(testDataHeaderPath: String,
			testDataPath : String,
			trustedDataHeaderPath : String,
			trustedDataPath : String)
			: OutputComparator = {
		// Get the data 
		// For the header the first entries is the id which we don't verify
		val testDataHeader = getHeader(testDataHeaderPath)
		val trustedDataHeader = getHeader(trustedDataHeaderPath)
		val testData =  getData(testDataPath)
		val trustedData = getData(trustedDataPath)

		new OutputComparator(testDataHeader, trustedDataHeader, testData, trustedData)
	}

	def apply (testDataHeader : Array[String],
			trustedDataHeader : Array[String],
			testData : org.apache.spark.rdd.RDD[(String, Array[String])],
			trustedData : org.apache.spark.rdd.RDD[(String, Array[String])]) 
			: OutputComparator ={
		new OutputComparator(testDataHeader, trustedDataHeader, testData, trustedData)
	}
}
