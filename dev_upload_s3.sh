#!/usr/bin/env bash

set -xv

cd target/scala-2.11/

files=(spark-seed_2.11*.jar)
jar_file="${files[0]}"

sha=$(git log --pretty=format:'%h' -n1)
ts=$(date +%s)
jar_name="${jar_file%.jar}"
dev_jar_name="${jar_name}_${sha}_${ts}.jar"

s3_path="s3://data-lake/dev/jars"
aws s3 cp "$jar_file" "${s3_path}/${dev_jar_name}"
