package util

import org.apache.log4j.Logger

trait Logging {
  val loggerName: String = this.getClass.getName
  val logger: Logger = Logger.getLogger(loggerName)
}
