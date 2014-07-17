import OutputComparator._
import report.CommandLineReport

object ResultsTest {
	def main(args: Array[String]){
		/*
		 * args: test-data-header test-data data-header data
		 */
		val comp = OutputComparator(args(0), args(1), args(2), args(3))
		CommandLineReport.report(comp)
	}
}
