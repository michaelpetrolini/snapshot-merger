package it.snapshot.merger.utils

import it.snapshot.merger.controller.MergeController

object SqlUtils {

  def describeTable(database: String, table: String): String = {
    s"describe table $database.$table"
  }

  def selectDistinct(mergeController: MergeController, lowerLimit: String, upperLimit: String): String = {
    s"select distinct ${mergeController.mergeDateFieldName} from ${mergeController.database}.${mergeController.table}" +
      s" where ${mergeController.mergeDateFieldName}>=$lowerLimit and ${mergeController.mergeDateFieldName}" +
      s"<=$upperLimit order by ${mergeController.mergeDateFieldName}"
  }

  def diffQuery(mergeController: MergeController, days: Seq[String], i: Int): String ={
    s"with differences as (with ${dayQuery(mergeController, days(i - 1), "yesterday")}, " +
      s"${dayQuery(mergeController, days(i), "today")} " +
      s"select * from yesterday full outer join today on ${joinCondition(mergeController)}) " +
      s"select *, ${generateStatusColumn(mergeController)} from differences"
  }

  def joinCondition(mergeController: MergeController): String ={
    mergeController.keyList.map(key => s"yesterday.yesterday_$key = today.today_$key").mkString(" and ")
  }

  def dayQuery(mergeController: MergeController, day: String, version: String): String ={
    s"$version as (select ${generateColumnsAsList(mergeController, version)} from " +
      s"${mergeController.database}.${mergeController.table} where ${mergeController.mergeDateFieldName}  = $day)"
  }

  def generateColumnsAsList(mergeController: MergeController, version: String): String ={
    mergeController.cList.map(cName => s"$cName as ${version}_$cName").mkString(", ")
  }

  def generateStatusColumn(mergeController: MergeController): String ={
    s"case when ${generateNullCondition(mergeController, "yesterday")} then 'NEW' " +
      s"when ${generateNullCondition(mergeController, "today")} then 'REMOVE' " +
      s"${editCase(mergeController)} else 'SAME' end as status"
  }

  def editCase(mergeController: MergeController): String = {
    val editableFields = mergeController.dataColumns.filter(_ != mergeController.mergeDateFieldName)
    if (editableFields.nonEmpty)
      s" when ${editableFields.map(cName => s"yesterday_$cName <> today_$cName").mkString(" or ")} then 'EDIT'"
    else
      ""
  }

  def generateNullCondition(mergeController: MergeController, version: String): String ={
    mergeController.keyList.map(cName => version + "_" + cName + " is null").mkString(" and ")
  }
}
