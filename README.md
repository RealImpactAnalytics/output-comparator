output-comparator
=================

Stand-alone spark tool to compare two datasets and generate a report.

Output
------
They are 2 possible output :

- CommandLine : via the object **CommandLineReport**. 
    This is the kind of output use by the main method in the object **ResultsTest**.
    (the script **run.sh** run the program with the commandLine output)

- ScalaTest : via the test command of **sbt** an example can be found in the script **test.sh**.
    The benefit, is that scalaTest can output a report as a **JUnitXml** which can be read by **Jenkins**.

