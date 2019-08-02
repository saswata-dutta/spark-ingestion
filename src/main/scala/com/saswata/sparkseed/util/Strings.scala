package com.saswata.sparkseed.util

object Strings {
  import scala.util.matching.Regex

  val NON_WORD_RE: Regex = """\W""".r
  val MULTI_US_RE: Regex = "_{2,}".r
  val safeChar: String = "_"

  def sanitise(str: String): String = {
    val trimmed = str.trim
    val underscored = NON_WORD_RE.replaceAllIn(trimmed, safeChar)
    val sanitised =
      MULTI_US_RE.replaceAllIn(underscored, safeChar)
    sanitised
  }

  def strictSanitise(str: String): String =
    sanitise(str).stripPrefix(safeChar).stripSuffix(safeChar)
}
