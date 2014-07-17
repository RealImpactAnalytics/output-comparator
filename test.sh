#!/bin/sh

sbt "test-only ComparisonTest -- -DtestDataHeader=data/test-data-header.csv -DtestData=data/test-data.csv  -DtrustedDataHeader=data/trusted-data-header.csv -DtrustedData=data/trusted-data.csv"

