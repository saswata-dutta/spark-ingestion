clean:
	sbt clean


lint: 
	sbt clean scapegoat scalafmtAll


build:
	sbt clean scapegoat scalafmtAll test assembly


upload-s3: build
	cd target/scala-2.11; aws s3 cp Spark-Seed_2.11-2.4.0_*.jar s3://rivigo-data-lake/dev/jars/