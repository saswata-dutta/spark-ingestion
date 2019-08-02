package com.saswata.sparkseed.util

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, ZoneId, ZonedDateTime}
import java.util.Locale

import scala.util.Try

object IstTime {
  val IST: ZoneId = ZoneId.of("Asia/Kolkata")

  final case class YMDHM(year: Int, month: Int, day: Int, hour: Int, minute: Int) {
    val y: String = "%04d".format(year)
    val m: String = "%02d".format(month)
    val d: String = "%02d".format(day)

    val ymdPartition: String = {
      s"y=$y/m=$m/d=$d"
    }

    @SuppressWarnings(Array("org.wartremover.contrib.warts.ExposedTuples"))
    val partitionValues: (String, String, String) = (y, m, d)
  }

  object YMDHM {

    def apply(ts: Long): YMDHM = {
      val zdt = guessEpochSecs(ts).map(epochSecsToZdt).getOrElse(now())
      apply(zdt)
    }

    def apply(zdt: ZonedDateTime): YMDHM =
      new YMDHM(zdt.getYear, zdt.getMonthValue, zdt.getDayOfMonth, zdt.getHour, zdt.getMinute)
  }

  val TIME_UNITS: Map[String, Long] =
    Map("s" -> 1L, "ms" -> 1000L, "us" -> 1000000L, "ns" -> 1000000000L)

  val TIME_SCALES: Map[String, Int] =
    Map("s" -> 10, "ms" -> 13, "us" -> 16, "ns" -> 19)

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

  private val EPOCH_SEC_THRESHOLD = 1000000000L

  /**
    * require at least  seconds level precision,
    * scale up or down according to the specified time unit
    *
    * @param epoch
    * @param timeUnit
    * @return the epoch scaled to the time unit
    */
  def guessEpoch(epoch: Any, timeUnit: String): Try[Long] = Try {
    val scale = TIME_SCALES(timeUnit)

    val current = new java.math.BigDecimal(epoch.toString)
    val currentScale = current.longValue.toString.length
    require(currentScale >= TIME_SCALES("s"), s"Too small epoch value $epoch")
    current.movePointRight(scale - currentScale).longValue
  }

  def guessEpochSecs(epoch: Any): Try[Long] =
    guessEpoch(epoch, "s")

  def guessEpochMillis(epoch: Any): Try[Long] =
    guessEpoch(epoch, "ms")

  def dateToZdt(date: LocalDate): ZonedDateTime = date.atStartOfDay(IST)

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def dateToEpoch(date: LocalDate, unit: String = "s"): Long = {
    val epochSecs = zdtToEpochSec(dateToZdt(date))
    scaleEpochSecs(epochSecs, unit)
  }

  def zdtToDate(zdt: ZonedDateTime): LocalDate = zdt.toLocalDate

  def epochSecsToDate(epochSecs: Long): LocalDate = zdtToDate(epochSecsToZdt(epochSecs))

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def epochToDate(epoch: Long, unit: String = "s"): LocalDate =
    zdtToDate(epochSecsToZdt(epoch / TIME_UNITS(unit)))

  def zdtToEpochSec(zdt: ZonedDateTime): Long = zdt.toEpochSecond

  def zdtToEpochMillis(zdt: ZonedDateTime): Long = zdt.toInstant.toEpochMilli

  def dateStrToEpochSecs(date: String): Long = {
    val zdt: ZonedDateTime = dateToZdt(LocalDate.parse(date, DATE_FORMATTER))
    zdt.toEpochSecond
  }

  def partitionFolder(date: String): String = {
    val dt = LocalDate.parse(date, DATE_FORMATTER)
    YMDHM(dt.getYear, dt.getMonthValue, dt.getDayOfMonth, 0, 0).ymdPartition
  }

  def scaleEpochSecs(epochSecs: Long, unit: String): Long =
    epochSecs * TIME_UNITS(unit)

  def partitionFolder(epoch: Long): String =
    YMDHM(epoch).ymdPartition

  def partitionFolder(zdt: ZonedDateTime): String =
    YMDHM(zdt).ymdPartition

  def dateStream(begin: LocalDate, end: LocalDate): Stream[LocalDate] =
    Stream.iterate(begin, ChronoUnit.DAYS.between(begin, end).toInt + 1)(_.plusDays(1))

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def dailyEpochs(startEpoch: Long, stopEpoch: Long, unit: String = "s"): Seq[Long] = {
    require(startEpoch < stopEpoch, s"$startEpoch not before $stopEpoch")

    val dates = dateStream(epochToDate(startEpoch, unit), epochToDate(stopEpoch, unit))
    dates.map(date => dateToEpoch(date, unit))
  }

  @SuppressWarnings(
    Array("org.wartremover.warts.DefaultArguments", "org.wartremover.contrib.warts.ExposedTuples")
  )
  def dailyEpochRanges(
    startEpoch: Long,
    stopEpoch: Long,
    unit: String = "s"
  ): Iterator[(Long, Long)] =
    dailyEpochs(startEpoch, stopEpoch, unit).sliding(2).map { l =>
      (l(0), l(1))
    }
}
