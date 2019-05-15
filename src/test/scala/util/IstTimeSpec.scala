package util

import org.scalatest.FlatSpec
import util.IstTime._

class IstTimeSpec extends FlatSpec {
  val sampleEpoch: Long = 1557645276L

  it should "compute y-m-d from epoch" in {
    val (y, m, d) = ymd(sampleEpoch)
    assert(y === 2019)
    assert(m === 5)
    assert(d === 12)
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

  it should "generate partition folders between epochs" in {
    val folders = partitionFolders(sampleEpoch, sampleEpoch + 259200L).toVector
    assert(folders === Vector("y=2019/m=05/d=12", "y=2019/m=05/d=13", "y=2019/m=05/d=14"))
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
  }
}
