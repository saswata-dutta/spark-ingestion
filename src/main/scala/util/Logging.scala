package util

import org.apache.log4j.Logger

trait Logging {
  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)
}