#!/bin/sh

~/spark-1.0.0/bin/spark-submit --class "ResultsTest" --master "local[4]"  target/scala-2.10/results-test_2.10-1.0.jar  data/test-data-header.csv data/test-data.csv  data/trusted-data-header.csv data/trusted-data.csv 

