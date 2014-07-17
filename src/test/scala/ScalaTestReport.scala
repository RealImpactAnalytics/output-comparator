import org.scalatest._
import OutputComparator._

abstract class SetSpec extends FunSpec with Matchers with BeforeAndAfterAll
class ComparisonTest extends SetSpec {

	// We want to compute the errors ony once for this class
	var comparator : OutputComparator = _

	/* 
	 * Use this method to access the config information before the test
	 * The before all don't run before the describe so we can't use it here
	 */
	override def run(testName: Option[String], args: Args): Status = {
		init(args.configMap)
		test()
		super.run(testName, args)
	}

	/*
	 * Initiatlise the comparator
	 */
	def init(configMap: org.scalatest.ConfigMap) {
		require(
			configMap.isDefinedAt("testDataHeader") &&
			configMap.isDefinedAt("testData")&&
			configMap.isDefinedAt("trustedDataHeader")&& 
			configMap.isDefinedAt("trustedData")
		)
		comparator = OutputComparator(configMap("testDataHeader").asInstanceOf[String], configMap("testData").asInstanceOf[String],
			configMap("trustedDataHeader").asInstanceOf[String], configMap("trustedData").asInstanceOf[String])
	}  

	def test() {
		describe("Check if they are missing column in the Trusted data"){
				comparator.missingTrustedColumn.foreach(h => 
					it(s"$h shouldn't be missing"){
						assert(false)
					})
		}

		describe("Check if they are missing column in the Test data"){
				comparator.missingTestColumn.foreach(h => 
					it(s"$h shouldn't be missing"){
						assert(false)
					})
		}

		describe("Verify the columns") { 
			// Print the error report
			comparator.columnErrorsPairs.foreach{ case (columnName, errors) =>
				errors match {
					case (Ok :: Nil) => 
						it(s"The column $columnName should be Ok"){
						}
					case errors => errors.foreach{
						case e : Error => 
							it(s"column : $columnName should be ok but for id: ${e.id} the value is ${e.testValue} but it should be ${e.trustedValue}"){
								assert(false)
							}
					}
				}
			}
		}
	}
}
