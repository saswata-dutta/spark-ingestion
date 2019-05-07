package util

import org.apache.log4j.Logger

trait Logging {
  val loggerName: String = this.getClass.getSimpleName
  val logger: Logger = Logger.getLogger(loggerName)
}
