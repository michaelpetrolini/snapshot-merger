package it.snapshot.merger.mongo

import it.snapshot.merger.utils.MergeUtils._
import com.mongodb.client.model.Filters
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}
import org.mongodb.scala.bson.collection.mutable
import org.mongodb.scala.model.Updates.{combine, inc, set}

import java.util.Date
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object MongoHelper {
  private val ConfigName = "configs"
  private val DatabaseName = "snapshot-merger"
  private val TableStructures = "table-structures"
  private val TablesState = "tables-state"
  private val IdSequence = "id-sequence"
  private val MergeJobs = "merge-jobs"
  private val MergeJobsDetails = "merge-jobs-details"
  private val MergeState = "merge-state"

  private val client: MongoClient = MongoClient()
  private val db: MongoDatabase = client.getDatabase(DatabaseName)
  private val config: MongoCollection[Document] = db.getCollection(ConfigName)
  private val tableStructures: MongoCollection[Document] = db.getCollection(TableStructures)
  private val tablesState: MongoCollection[Document] = db.getCollection(TablesState)
  private val idSequence: MongoCollection[Document] = db.getCollection(IdSequence)
  private val mergeJobs: MongoCollection[Document] = db.getCollection(MergeJobs)
  private val mergeJobsDetails: MongoCollection[Document] = db.getCollection(MergeJobsDetails)
  private val mergeState: MongoCollection[Document] = db.getCollection(MergeState)
  private val idMergeJob = setMergeJobId()

  def getMergeJobs(inputs: HashMap[String, String]): Seq[Document] = {
    val filter = mutable.Document()

    if (inputs("database") != "")
      filter.put("database", inputs("database"))

    if (inputs("table") != "")
      filter.put("table", inputs("table"))

    val mergeJobs = tableStructures.find(filter)
    Await.result(mergeJobs.toFuture(), Duration.Inf)
  }

  def getMergeDaysList(database: String, table: String, mergeDays: Int,
                       arguments: Map[String, String]): Seq[String] = {
    val lastDate = getLastMergeState(database, table)

    val lastUpdate = if (lastDate.isDefined) lastDate.get.getString("date") else "00000000"
    val dayLimit = subtractFromToday(mergeDays)
    val startDate = if(arguments("startDate") != "") arguments("startDate") else "00000000"
    val endDate = if(arguments("endDate") != "") arguments("endDate") else "99999999"
    val upperLimit = if (dayLimit.toInt < endDate.toInt) dayLimit else endDate
    val lowerLimit = if (lastUpdate.toInt > startDate.toInt) lastUpdate else startDate

    val jobFilter = Filters.and(Filters.eq("database", database),
                                Filters.eq("table", table),
                                Filters.gte("date", lowerLimit),
                                Filters.lte("date", upperLimit))

    var daysList = Await.result(tablesState.distinct[String]("date", jobFilter).toFuture(), Duration.Inf).sorted

    if (lastDate.isEmpty) {
      daysList = "00000000" +: daysList
    } else if (daysList.head != lastUpdate) {
      val lastBulkFlag: Boolean = if (lastDate.isDefined) lastDate.get.getBoolean("bulkFlag") else false
      if (lastBulkFlag)
        daysList = daysList.updated(0, lastUpdate)
      else
        daysList = lastUpdate +: daysList
    }
    daysList
  }

  def createMergeState(database: String, table: String, referenceDate: String): Unit = {
    val newDoc = Document("jobId" -> idMergeJob,
                          "database" -> database,
                          "table" -> table,
                          "date" -> referenceDate,
                          "start" -> new Date(),
                          "status" -> "ONGOING")
    Await.result(mergeState.insertOne(newDoc).toFuture(), Duration.Inf)
  }

  def setSuccessMergeState(database: String, table: String, date: String, nEdit: Long, nNew: Long, nRemove: Long,
                           nTotal: Long, currentRecords: Long): Unit = {
    val filter = Filters.and(Filters.eq("jobId", idMergeJob),
                             Filters.eq("database", database),
                             Filters.eq("table", table),
                             Filters.eq("date", date))

    val update = combine(set("nEdit", nEdit), set("nNew", nNew), set("nRemove", nRemove), set("nTotal", nTotal),
              set("currentRecords", currentRecords), set("status", "SUCCESS"), set("end", new Date()))

    Await.result(mergeState.updateOne(filter, update).toFuture(), Duration.Inf)
  }

  def setFailedMergeState(database: String, table: String, date: String): Unit = {
    val filter = Filters.and(Filters.eq("jobId", idMergeJob),
                             Filters.eq("database", database),
                             Filters.eq("table", table),
                             Filters.eq("date", date))
    val update = combine(set("end", new Date()), set("status", "ERROR"))

    Await.result(mergeState.updateOne(filter, update).toFuture(), Duration.Inf)
  }

  def deleteMergeState(database: String, table: String, referenceDate: String): Unit = {
    val remove = Filters.and(Filters.eq("database", database),
                             Filters.eq("table", table),
                             Filters.eq("date", referenceDate))

    Await.result(mergeState.deleteOne(remove).toFuture(), Duration.Inf)
  }

  private def setMergeJobId(): String = {
    val id = Await.result(idSequence.findOneAndUpdate(Filters.eq("_id", "merge"),
                                                      inc("value", 1)).toFuture(),
                          Duration.Inf).getInteger("value")
    "DS_" + id
  }

  def initMergeJobTracer(applicationId: String, verifiedSources: Int): Unit = {
    val job = Document("jobId" -> idMergeJob, "startTime" -> new Date(), "applicationId" -> applicationId,
                       "status" -> "ONGOING", "verifiedSources" -> verifiedSources, "processedSources" -> 0,
                       "loadedPartitions" -> 0)
    Await.result(mergeJobs.insertOne(job).toFuture(), Duration.Inf)
  }

  def createMergeJobInstance(database: String, table: String, referenceDate: String): Unit = {
    val jobInstance = Document("jobId" -> idMergeJob, "database" -> database, "table" -> table, "source" -> "entity",
                               "date" -> referenceDate, "status" -> "ONGOING", "startTime" -> new Date())
    Await.result(mergeJobsDetails.insertOne(jobInstance).toFuture(), Duration.Inf)
  }

  def createMultiMergeJobInstances(database: String, table: String, referenceDates: Seq[String], source: String,
                                   tableType: String, bulkFlag: Boolean): Unit = {
    val docList = new ListBuffer[Document]
    for(i <- 1 until referenceDates.size) {
      docList.append(Document("jobId" -> idMergeJob, "database" -> database, "table" -> table, "source" -> "entity",
        "date" -> referenceDates(i), "status" -> "ONGOING", "startTime" -> new Date(), "source" -> source,
        "type" -> tableType, "bulkFlag" -> bulkFlag))
    }
    Await.result(mergeJobsDetails.insertMany(docList.toList).toFuture(), Duration.Inf)
  }

  def completeJobInstance(database: String, table: String, referenceDate: String, nJobs: Int): Unit = {
    val filter = Filters.and(Filters.eq("jobId", idMergeJob),
                             Filters.eq("database", database),
                             Filters.eq("table", table),
                             Filters.eq("date", referenceDate))
    val doc = Await.result(mergeJobsDetails.find(filter).toFuture(), Duration.Inf).head
    val endTime = new Date()
    val duration = (endTime.getTime - doc.getDate("startTime").getTime) / (1000*nJobs)
    val update = combine(set("status", "OK"), set("endTime", endTime), set("duration", duration))
    Await.result(mergeJobsDetails.updateOne(filter, update).toFuture(), Duration.Inf)
  }

  def completeMultiJobInstances(ambit: String, table: String, referenceDates: Seq[String]): Unit = {
    for(i <- 1 until referenceDates.size){
      completeJobInstance(ambit, table, referenceDates(i), referenceDates.size - 1)
    }
  }

  def updateProcessedSources(): Unit = {
    val filter = Filters.eq("jobId", idMergeJob)
    val update = inc("processedSources", 1)
    Await.result(mergeJobs.updateOne(filter, update).toFuture(), Duration.Inf)
  }

  def updateLoadedPartitions(): Unit = {
    val filter = Filters.eq("jobId", idMergeJob)
    val update = inc("loadedPartitions", 1)
    Await.result(mergeJobs.updateOne(filter, update).toFuture(), Duration.Inf)
  }

  def updateLoadedPartitions(increment: Int): Unit = {
    val filter = Filters.eq("jobId", idMergeJob)
    val update = inc("loadedPartitions", increment)
    Await.result(mergeJobs.updateOne(filter, update).toFuture(), Duration.Inf)
  }

  def closeJobTracer(nCompleted: Int, nTotal: Int): Unit = {
    val filter = Filters.eq("jobId", idMergeJob)
    val doc = Await.result(mergeJobs.find(filter).toFuture(), Duration.Inf).head
    val endTime = new Date()
    val duration = (endTime.getTime - doc.getDate("startTime").getTime) / 1000
    val status = if (nCompleted == nTotal) "OK" else if (nCompleted == 0) "FAILED" else "WARNING"
    val update = combine(set("status", status), set("endTime", endTime), set("duration", duration))
    Await.result(mergeJobs.updateOne(filter, update).toFuture(), Duration.Inf)
  }

  def manageFailedInstance(database: String, table: String, referenceDate: String, e: Exception): Unit = {
    val filter = Filters.and(Filters.eq("jobId", idMergeJob),
                             Filters.eq("database", database),
                             Filters.eq("table", table),
                             Filters.eq("date", referenceDate))
    val update = combine(set("status", "FAILED"), set("errorMessage", e.getMessage))
    Await.result(mergeJobsDetails.updateOne(filter, update).toFuture(), Duration.Inf)
  }

  def manageMultiFailedInstances(database: String, table: String, dates: Seq[String], e: Exception): Unit = {
    for(i <- 1 until dates.size){
      manageFailedInstance(database, table, dates(i), e)
    }
  }

  def getLastMergeState(database: String, table: String): Option[Document] = {
    val statusFilter = Filters.and(Filters.eq("database", database),
                                   Filters.eq("table", table),
                                   Filters.eq("status", "SUCCESS"))
    Await.result(mergeState.find(statusFilter).sort(Document("date" -> -1)).limit(1).toFuture(),
                 Duration.Inf).headOption
  }

  def getLastErrorDate(database: String, table: String): Option[String] = {
    val statusFilter = Filters.and(Filters.eq("database", database),
                                   Filters.eq("table", table))

    val lastDay = Await.result(mergeState.find(statusFilter).sort(Document("date" -> -1)).limit(1).toFuture(),
                               Duration.Inf).headOption
    if (lastDay.isDefined && lastDay.get.getString("status") != "SUCCESS")
      Some(lastDay.get.getString("date"))
    else
      None
  }

  def getMergeConfig: Document= {
    val configFilter = Filters.eq("id", "merge-config")
    Await.result(config.find(configFilter).toFuture(), Duration.Inf).head
  }
}
