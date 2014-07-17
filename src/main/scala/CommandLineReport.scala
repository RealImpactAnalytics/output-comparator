import OutputComparator._

object CommandLineReport extends TestResultReport {

	def report(comparator : OutputComparator){

		println("Trusted missing column :")
		comparator.missingTrustedColumn.foreach(h => println(h))
		println("Test missing column :")
		comparator.missingTestColumn.foreach(h => println(h))

		// Print the error report
		comparator.columnErrorsPairs.foreach{ case (columnName, errors) =>
			errors match {
				case (Ok :: Nil) => println(s"column : $columnName is OK")
				case errors => errors.foreach{
					case e : Error => println(s"column : $columnName error : for id : ${e.id} the value is ${e.testValue} but it should be ${e.trustedValue}")
				}
			}
		}
	}
}
