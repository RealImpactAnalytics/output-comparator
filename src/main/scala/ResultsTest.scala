object ResultsTest {

	/**
	 * Execute the tests on the given files
	 * @param args test-data-header test-data data-header data
	 */
	def main(args: Array[String]){
		val comp = OutputComparator(args(0), args(1), args(2), args(3))
		CommandLineReport.report(comp)
	}
}
