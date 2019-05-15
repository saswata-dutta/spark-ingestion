package util

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
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
    ymd(zdt)
  }

  def ymd(zdt: ZonedDateTime): (Int, Int, Int) =
    (zdt.getYear, zdt.getMonthValue, zdt.getDayOfMonth)

  private val EPOCH_SEC_LEN = 10
  private val EPOCH_SEC_THRESHOLD = 1000000000L
  private val EPOCH_MS_LEN = 13
  private val EPOCH_MS_THRESHOLD = 1000000000000L

  private def guessEpoch(epoch: Any, len: Int, threshold: Long): Try[Long] =
    Try(epoch.toString.take(len).toLong).filter(_ > threshold)

  def guessEpochSecs(epoch: Any): Try[Long] =
    guessEpoch(epoch, EPOCH_SEC_LEN, EPOCH_SEC_THRESHOLD)

  def guessEpochMillis(epoch: Any): Try[Long] =
    guessEpoch(epoch, EPOCH_MS_LEN, EPOCH_MS_THRESHOLD)

  def dateToZdt(date: LocalDate): ZonedDateTime = date.atStartOfDay(IST)

  def zdtToDate(zdt: ZonedDateTime): LocalDate = zdt.toLocalDate

  def epochSecsToDate(epochSecs: Long): LocalDate = zdtToDate(epochSecsToZdt(epochSecs))

  def zdtToEpochSec(zdt: ZonedDateTime): Long = zdt.toEpochSecond

  def zdtToEpochMillis(zdt: ZonedDateTime): Long = zdt.toInstant.toEpochMilli

  def dateStrToEpochSecs(date: String): Long = {
    val zdt: ZonedDateTime = dateToZdt(LocalDate.parse(date, DATE_FORMATTER))
    zdt.toEpochSecond
  }

  def scaleEpochSecs(epochSecs: Long, unit: String): Long =
    epochSecs * TIME_UNITS(unit)

  def partitionFolder(epoch: Long): String =
    guessEpochSecs(epoch)
      .map(e => ymdPartition.tupled(ymd(e)))
      .getOrElse("")

  def partitionFolder(zdt: ZonedDateTime): String = {
    val epochSecs = zdtToEpochSec(zdt)
    ymdPartition.tupled(ymd(epochSecs))
  }

  val ymdPartition: (Int, Int, Int) => String = (y, m, d) => f"y=$y%04d/m=$m%02d/d=$d%02d"

  def dateStream(begin: LocalDate, end: LocalDate): Stream[LocalDate] =
    Stream.iterate(begin, ChronoUnit.DAYS.between(begin, end).toInt)(_.plusDays(1))

  def partitionFolders(beginEpochSecs: Long, endEpochSecs: Long): Seq[String] = {
    val begin = epochSecsToDate(beginEpochSecs)
    val end = epochSecsToDate(endEpochSecs)
    val dates = dateStream(begin, end)
    dates.map(date => ymdPartition(date.getYear, date.getMonthValue, date.getDayOfMonth))
  }
}
