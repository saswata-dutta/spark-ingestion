package com.saswata.sparkseed.util

import java.time.ZonedDateTime

import com.saswata.sparkseed.util.IstTime._
import org.scalatest.FlatSpec

class IstTimeSpec extends FlatSpec {
  val sampleEpoch: Long = 1557645276L

  it should "compute y-m-d from epoch" in {
    val folder = partitionFolder(sampleEpoch)
    assert(folder === "y=2019/m=05/d=12")
  }

  it should "guess Epoch Secs" in {
    assert(guessEpochSecs(sampleEpoch).get === sampleEpoch)
    assert(guessEpochSecs(sampleEpoch * 1000 + 123).get === sampleEpoch)
    assert(guessEpochSecs(sampleEpoch * 1000000 + 123123).get === sampleEpoch)
    assert(guessEpochSecs(sampleEpoch.toString).get === sampleEpoch)
    assert(guessEpochSecs(sampleEpoch.toString + ".0").get === sampleEpoch)
    assert(guessEpochSecs(sampleEpoch.toString + ".123").get === sampleEpoch)
    assert(guessEpochSecs(sampleEpoch.toString + ".123456").get === sampleEpoch)
    assert(guessEpochSecs(sampleEpoch.toString + "123").get === sampleEpoch)
    assert(guessEpochSecs(sampleEpoch.toString + "123456").get === sampleEpoch)
    assert(guessEpochSecs(1562064430256038.0).get === 1562064430L)
    assert(guessEpochSecs(1562064430256.0380).get === 1562064430L)
  }

  it should "guess Epoch Ms" in {
    val sampleMillis: Long = 1557645276123L
    assert(guessEpochMillis(sampleMillis).get === sampleMillis)
    assert(guessEpochMillis(sampleMillis * 1000 + 123).get === sampleMillis)
    assert(guessEpochMillis(sampleMillis * 1000000 + 123123).get === sampleMillis)
    assert(guessEpochMillis(sampleMillis.toString).get === sampleMillis)
    assert(guessEpochMillis(sampleMillis.toString + ".0").get === sampleMillis)
    assert(guessEpochMillis(sampleMillis.toString + ".123").get === sampleMillis)
    assert(guessEpochMillis(sampleMillis.toString + ".123456").get === sampleMillis)
    assert(guessEpochMillis(sampleMillis.toString + "123").get === sampleMillis)
    assert(guessEpochMillis(sampleMillis.toString + "123456").get === sampleMillis)
    assert(guessEpochMillis(1562064430256038.0).get === 1562064430256L)
    assert(guessEpochMillis(1562064430256.0380).get === 1562064430256L)
  }

  it should "guess Epoch Ms from secs" in {
    assert(guessEpochMillis(sampleEpoch).get === sampleEpoch * 1000L)
    assert(guessEpochMillis(sampleEpoch / 10.0).isFailure)
  }

  it should "scale Epoch Ms up and down" in {
    assert(guessEpochMillis("1234567890").get === 1234567890000L)
    assert(guessEpochMillis("1234567890.123").get === 1234567890123L)
    assert(guessEpochMillis("1234567890.123456").get === 1234567890123L)
    assert(guessEpochMillis("1234567890.123456789").get === 1234567890123L)
    assert(guessEpochMillis("12345678901.23456789").get === 1234567890123L)
    assert(guessEpochMillis("123456789012.3456789").get === 1234567890123L)
    assert(guessEpochMillis("1234567890123456.789").get === 1234567890123L)
    assert(guessEpochMillis("1234567890123456789.0123").get === 1234567890123L)
  }

  it should "convert epochSecs To Zdt" in {
    val ans = epochSecsToZdt(sampleEpoch)
    assert(ans.getYear === 2019)
    assert(ans.getMonthValue === 5)
    assert(ans.getDayOfMonth === 12)
    assert(ans.getHour === 12)
    assert(ans.getMinute === 44)
    assert(ans.getSecond === 36)
    assert(ans.getZone === IST)
  }

  it should "convert dateStr To EpochSecs" in {
    assert(dateStrToEpochSecs("2019-05-12") === 1557599400)
  }

  it should "scale Epoch Secs" in {
    assert(scaleEpochSecs(sampleEpoch, "s") === sampleEpoch)
    assert(scaleEpochSecs(sampleEpoch, "ms") === sampleEpoch * 1000L)
    assert(scaleEpochSecs(sampleEpoch, "us") === sampleEpoch * 1000000L)
    assert(scaleEpochSecs(sampleEpoch, "ns") === sampleEpoch * 1000000000L)
  }

  it should "generate partition from epoch" in {
    val epochMs: Long = 1557645276123123L
    assert(partitionFolder(epochMs) === "y=2019/m=05/d=12")
  }

  it should "generate partition from date" in {
    assert(partitionFolder("2019-01-21") === "y=2019/m=01/d=21")
  }

  it should "convert epoch to date and vice-versa in ist" in {
    val epochMillis = 1559692799000L // 05:29:59
    val date = epochToDate(epochMillis, "ms")

    val (y, m, d) = (2019, 6, 5)
    assert(date.getYear === y)
    assert(date.getMonthValue === m)
    assert(date.getDayOfMonth === d)

    val startOfDayMillis =
      ZonedDateTime.of(y, m, d, 0, 0, 0, 0, IST).toInstant.toEpochMilli
    assert(dateToEpoch(date, "ms") === startOfDayMillis)
  }

  it should "generate daily epoch range between 2 epochs" in {
    val range1 =
      dailyEpochRanges(dateStrToEpochSecs("2019-01-01"), dateStrToEpochSecs("2019-01-02")).toVector
    assert(range1 === Vector((1546281000L, 1546367400L)))

    val threeDaysSecs = 259200L
    val range2 =
      dailyEpochRanges(sampleEpoch, sampleEpoch + threeDaysSecs).toVector
    assert(
      range2 === Vector(
          (1557599400, 1557685800),
          (1557685800, 1557772200),
          (1557772200, 1557858600)
        )
    )
  }
}
