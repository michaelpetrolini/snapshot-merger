package it.snapshot.merger.utils

import org.apache.hadoop.hbase.util.MD5Hash

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.util.Date

object MergeUtils {
  def subtractFromToday(daysToSubtract: Long): String = {
    new SimpleDateFormat("yyyyMMdd").format(Date.from(LocalDate.now().
                                                              minusDays(daysToSubtract).
                                                                        atStartOfDay(ZoneId.systemDefault()).toInstant))
  }

  def getPreviousDay(date: String): String = {
    subtractFromDate(date, 1)
  }

  def subtractFromDate(date: String, daysToSubtract: Long): String = {
    new SimpleDateFormat("yyyyMMdd").format(Date.from(LocalDate.of(date.substring(0, 4).toInt,
                                                                            date.substring(4, 6).toInt,
                                                                            date.substring(6, 8).toInt).
                                                              minusDays(daysToSubtract).
                                                              atStartOfDay(ZoneId.systemDefault()).toInstant))
  }

  def getHashValue(pk: String): String = {
    MD5Hash.getMD5AsHex(pk.getBytes()).take(3)
  }
}
