# Spark-Ingestion

Spark based framework to ingest data from any source, and flush to any sink.

## Code

The code is designed to be used as a fat jar to be consumed via `spark-submit`.

`BaseDriver` is the app entry point, to be extended by each jop object.
* defines the job trait,
* reads App configs based on the `com.saswata.sparkseed.config` file name passed in as cmd line arg to program,
* creates the spark context,
* configures `com.saswata.sparkseed.sources` and `com.saswata.sparkseed.sinks`
* calls the `run` method to execute

`AppConfig` contains all the HOCON configs defined.

`BaseSource` reads a dataFrame based on the configs.

`BaseSink` writes a dataFrame based on the configs.

Checkout `GenericIngestJob` and `GenericTransformers` for usages.
Refer `transforms.Udfs` for preexisting spark sql functions. 

## Make

The `makefile`  has recipes for linting, building the fat jar, and uploading it to s3, so it can be used in qubole/spark clusters.

## Job configs

Check out `trip_events.conf` for a working sample of the config file.
The `reference.conf` and `local.conf` (if the `--local` arg is passed) are merged with the main job conf to provide additional common configs.

```hocon

source {
  // for mongo
  type = "mongo"
  host = "host:port" // assumes auth db is admin
  ssm-path = "/prefix/of/aws/ssm/mysql/credentials/"
  user = "user" // use in local
  password = "pass" // use in local
  database = "data-base"
  table = "some_table"
  readPreference.name = "secondaryPreferred" // optional, but good to have
  sampleSize = 10000000 // optional for better schema infer, use sensibly
  
  // or for mysql
  type = "jdbc"
  url = "jdbc:mysql://mysql-host:port/db-name"
  dbtable = "db-name.tbl-name"
  ssm-path = "/prefix/of/aws/ssm/mysql/credentials/"
  user = "user" // use in local
  password = "pass" // use in local
  fetchsize = "20000" // optional 
}

sink {
  type = "s3"
  bucket = "bucket-name"
  folder = "some/path/to"
  format = "parquet"
  save_mode = "error or overwrite = equvalent savemode"
}

spark {
  master = "local[*]" // overrdide defaults if needed
  // additional spark configs
}

schema {
  time_partition_col = "col by which the data is to be partitioned"
  time_partition_col_unit = "unit of time s/ms/us/ns"
  epoch_cols = ["cols which must be sanitised as epochs"] // optional
  numeric_cols = ["cols which must be sanitised as doubles"] // optional
  bool_cols = ["cols which must be sanitised as bools"] // optional
  flatten = false // optional, flatten StrucType-s
}


```

## Command Args
```
spark-submit --class com.saswata.sparkseed.drivers.GenericIngestJob --conf-file trip_events.conf --start-epoch 1557340200 --stop-epoch 1557426600
```
### Args:
```
--class "canonical name of class for job"
--conf "conf to submit to spark"
--conf-file "name of conf file for this job"
--start-epoch "start time of data partition in seconds"
--stop-epoch "stop time of data partition in seconds"
--start-date "start time of data partition in yyyy-mm-dd"
--stop-date "stop time of data partition in yyyy-mm-dd"
--local "pass 'true' to run locally in intellij, using the 'mainRunner' config"
```
