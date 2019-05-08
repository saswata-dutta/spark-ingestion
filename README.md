# spark-seed

BaseDriver 
* defines the job trait,
* reads App configs based on the config file name passed in as cmd line arg to program,
* creates the spark context,
* configures sources and sinks
* calls the 'run' method to execute

AppConfig contains all the HOCON configs defined
BaseSource, reads a dataFrame based on the configs.s
BaseSink, writes a dataFrame based on the configs.
