package util

import java.time.{Instant, ZoneId, ZonedDateTime}

object IstTime {
  private val IST = ZoneId.of("Asia/Kolkata")

  private def currentInstant(): Instant = Instant.now()
  def now(): ZonedDateTime = currentInstant().atZone(IST)
  def currentMillis(): Long = currentInstant().toEpochMilli
  def fromEpochSeconds(epochSeconds: Long): ZonedDateTime = {
    val i = Instant.ofEpochSecond(epochSeconds)
    ZonedDateTime.ofInstant(i, IST)
  }

  def ymd(epochSeconds: Long): (Int, Int, Int) = {
    val dt = fromEpochSeconds(epochSeconds)
    (dt.getYear, dt.getMonthValue, dt.getDayOfMonth)
  }
}
