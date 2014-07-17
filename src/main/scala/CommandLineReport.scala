import OutputComparator._

object CommandLineReport extends TestResultReport {

	def report(comparator : OutputComparator){
		// Print the error report
		comparator.columnMapping.zip(comparator.synthethicTestResults).foreach{case ((trustedColumnIndex, _), errors) =>
			val columnName = comparator.trustedDataHeader(trustedColumnIndex)
			errors match {
				case (Ok :: Nil) => println(s"column : $columnName is OK")
				case errors => errors.foreach{
					case e : Error => println(s"column : $columnName error : for id : ${e.id} the value is ${e.testValue} but it should be ${e.trustedValue}")
				}
			}
		}

		println("Trusted missing column :")
		comparator.missingTrustedColumn.foreach(i => println(comparator.trustedDataHeader(i)))
		println("Test missing column :")
		comparator.missingTestColumn.foreach(i => println(comparator.testDataHeader(i)))

		println("results : ")
		// Print some results
		comparator.results.take(10).foreach(_.zipWithIndex.foreach{
			case (List(Ok), i) => println(s"column : ${comparator.testDataHeader(i)} OK")
			case (List(Error(id, trustedValue, testValue, msg)), i) => println(msg)
		})
	}
}
