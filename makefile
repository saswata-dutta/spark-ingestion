clean:
	sbt clean


lint:
	sbt clean scalastyle test:scalastyle scapegoat scalafmtCheckAll test


build:
	sbt clean scalastyle test:scalastyle scapegoat scalafmtCheckAll assembly


upload-s3: build
	./dev_upload_s3.sh
