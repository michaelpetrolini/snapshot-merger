package it.snapshot.merger.controller

import com.typesafe.scalalogging.LazyLogging
import it.snapshot.merger.config.MergeConfig
import it.snapshot.merger.mongo.MongoHelper
import it.snapshot.merger.utils.HBaseUtils._
import it.snapshot.merger.utils.SqlUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mongodb.scala.Document

import scala.collection.immutable.HashMap
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}

class MergeController(var doc: Document, var spark: SparkSession, var inputs: HashMap[String, String])
  extends Serializable with LazyLogging {
  val arguments: Map[String, String] = inputs
  val database: String = doc.getString("database")
  val table: String = doc.getString("table")
  val delay: Int = doc.getInteger("delayDays")
  val keyList: Array[String] = doc.getList("dataKey", classOf[String]).asScala.toArray
  val cList: Array[String] = spark.sql(SqlUtils.describeTable(database, table)).select("col_name").
                                      rdd.map(x => x.getString(0)).collect()
  val dataColumns: Array[String] = cList.toSet.diff(keyList.toSet).toArray
  val mergeDateFieldName: String = "merge_date"
  val hbaseTableName: String = MergeConfig.hBaseNamespace + ":" + database + "-" + table

  @transient val conf: Configuration = HBaseConfiguration.create()
  conf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

  var totalCounter = 0
  var completedCounter = 0

  def manageMergeUpdate(): Unit ={
    manageNewHBaseTable(this)
    logger.info(s"HBase table creation $database-$table completed.")
    val errorDate = MongoHelper.getLastErrorDate(database, table)

    if(errorDate.isDefined){
      logger.warn(s"Rollback of date ${errorDate.get} to the previous table's date" +
        s" a Stock $database-$table in corso")
      rollBack(this, spark, errorDate.get)
      MongoHelper.deleteMergeState(database, table, errorDate.get)
      logger.info(s"Rollback of table $database-$table completed.")
    }

    val days = getMergeDays
    if (days.size > 1) {
      totalCounter = days.size - 1
      MongoHelper.updateProcessedSources()
      breakable {
        for (i <- 1 until days.size) {
          try {
            logger.info(s"Starting the merge process for the snapshot of date ${days(i)} to the table $database-$table")
            MongoHelper.createMergeJobInstance(database, table, days(i))
            MongoHelper.createMergeState(database, table, days(i))
            val lastDate = MongoHelper.getLastMergeState(database, table)

            val differences = spark.sql(SqlUtils.diffQuery(this, days, i))
            val hbasePuts = differences.filter("status != 'SAME'")

            if (lastDate.isEmpty)
              bulkLoadFirstDate(this, days(i), hbasePuts)
            else
              singlePutInsertion(this, days(i), hbasePuts)

            MongoHelper.completeJobInstance(database, table, days(i), 1)
            MongoHelper.updateLoadedPartitions()
            setMergeDetails(differences, days(i))
            completedCounter += 1
            logger.info(s"The merge process for the snapshot of date ${days(i)} of the table $database-$table is completed.")
          }
          catch {
            case e: Exception =>
              logger.error(s"Error during the merge process for the snapshot of date ${days(i)}" +
                s" of the table $database-$table: $e")
              MongoHelper.manageFailedInstance(database, table, days(i), e)
              MongoHelper.setFailedMergeState(database, table, days(i))
              break
          }
        }
      }
    } else
      logger.info(s"No snapshots found for the table $database-$table.")
  }

  def getMergeDays: Seq[String] = {
    var tmpList = Array[String]()
    tmpList = MongoHelper.getMergeDaysList(database, table, delay, arguments).toArray
    if(arguments("maxDays") != "") tmpList.take(arguments("maxDays").toInt + 1) else tmpList
  }

  def setMergeDetails(df: DataFrame, day: String): Unit = {
    val nEdit: Long = df.filter("status == 'EDIT'").count
    val nNew: Long = df.filter("status == 'NEW'").count
    val nRemove: Long = df.filter("status == 'REMOVE'").count
    val nTotal: Long = nEdit + nNew + nRemove
    val currentRecords: Long = df.count - nRemove

    MongoHelper.setSuccessMergeState(database, table, day, nEdit, nNew, nRemove, nTotal, currentRecords)
  }
}
