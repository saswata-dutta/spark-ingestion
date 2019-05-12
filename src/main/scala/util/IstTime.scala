package util

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneId, ZonedDateTime}
import java.util.Locale

import scala.util.Try

object IstTime {
  val IST: ZoneId = ZoneId.of("Asia/Kolkata")

  val TIME_UNITS: Map[String, Long] =
    Map("s" -> 1L, "ms" -> 1000L, "us" -> 1000000L, "ns" -> 1000000000L)
  val DATE_PATTERN: String = "yyyy-MM-dd"
  val DATE_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern(DATE_PATTERN, Locale.ENGLISH)

  private def currentInstant(): Instant = Instant.now()
  def now(): ZonedDateTime = currentInstant().atZone(IST)
  def currentMillis(): Long = currentInstant().toEpochMilli

  def epochSecsToZdt(epochSeconds: Long): ZonedDateTime = {
    require(epochSeconds >= EPOCH_SEC_THRESHOLD)
    val i = Instant.ofEpochSecond(epochSeconds)
    ZonedDateTime.ofInstant(i, IST)
  }

  def ymd(epochSeconds: Long): (Int, Int, Int) = {
    val zdt: ZonedDateTime = epochSecsToZdt(epochSeconds)
    (zdt.getYear, zdt.getMonthValue, zdt.getDayOfMonth)
  }

  private val EPOCH_SEC_LEN = 10
  private val EPOCH_SEC_THRESHOLD = 1000000000L

  def guessEpochSecs(epoch: Any): Try[Long] =
    Try(epoch.toString.take(EPOCH_SEC_LEN).toLong).filter(_ > EPOCH_SEC_THRESHOLD)

  def dateStrToEpochSecs(date: String): Long = {
    val zdt: ZonedDateTime = LocalDate.parse(date, DATE_FORMATTER).atStartOfDay(IST)
    zdt.toEpochSecond
  }

  def scaleEpochSecs(epochSecs: Long, unit: String): Long =
    epochSecs * TIME_UNITS(unit)

  def partitionFolder(epoch: Long): String =
    guessEpochSecs(epoch)
      .map(e => ymd(e))
      .map {
        case (y, m, d) => f"y=$y%04d/m=$m%02d/d=$d%02d"
      }
      .getOrElse("")
}
