package it.snapshot.merger.utils

import it.snapshot.merger.config.MergeConfig
import it.snapshot.merger.controller.MergeController
import it.snapshot.merger.utils.MergeUtils.{getHashValue, getPreviousDay}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{FilterList, FirstKeyOnlyFilter, KeyOnlyFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util
import scala.collection.mutable.ListBuffer

object HBaseUtils {
  def setVersion(put: Put, value: Result): Put = {
    val version = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("info"),
      Bytes.toBytes("version")))).toInt
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("version"), Bytes.toBytes((version + 1).toString))
  }

  def getPreviousHBaseRow(table: Table, pk: String): Option[Result] = {
    val scan = new Scan().setLimit(1).setReversed(true).
      withStartRow(Bytes.unsignedCopyAndIncrement(Bytes.toBytes(s"sk_${getHashValue(pk)}_$pk"))).
      withStopRow(Bytes.toBytes("sk_" + getHashValue(pk) + "_" + pk))
    val scanResult = Option(table.getScanner(scan).next())
    var rowKey: Array[Byte] = Array[Byte]()
    scanResult match {
      case Some(scanValue) =>
        rowKey = CellUtil.cloneValue(scanValue.getColumnLatestCell(Bytes.toBytes("info"), Bytes.toBytes("mergeKey")))
        val result = table.get(new Get(rowKey))
        if (result.isEmpty)
          None
        else
          Some(result)
      case None =>
        None
    }
  }

  def createPutObject(cList: Array[String], row: Row, pk: String, date: String, version: String, isDeleted: Boolean): Put = {
    val put = new Put(Bytes.toBytes(s"pk_${getHashValue(date)}_${date}_$pk"))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dateFrom"), Bytes.toBytes(date))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("dateTo"), Bytes.toBytes("99999999"))
    val deleteFlag = if (isDeleted) "1" else "0"
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("deleteFlag"), Bytes.toBytes(deleteFlag))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("uniqueId"), Bytes.toBytes(pk + "_" + date))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pk"), Bytes.toBytes(pk))
    cList.foreach(cName =>
      if (!row.isNullAt(row.fieldIndex(version + "_" + cName)) &&
          row.get(row.fieldIndex(version + "_" + cName)).toString.nonEmpty)
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes(cName),
          Bytes.toBytes(row.get(row.fieldIndex(version + "_" + cName)).toString))
    )
    put
  }

  def createKeyObject(pk: String, date: String): Put = {
    val keyObject = new Put(Bytes.toBytes(s"sk_${getHashValue(pk)}_${pk}_$date"))
    keyObject.addColumn(Bytes.toBytes("info"), Bytes.toBytes("mergeKey"), Bytes.toBytes(s"pk_${getHashValue(date)}_${date}_$pk"))
    keyObject
  }

  def manageNewRow(mc: MergeController, day: String, row: Row, table: Table, putList: util.ArrayList[Put]): Unit = {
    val pk = assembleRowKey(mc.keyList, row, "today")
    val put: Put = createPutObject(mc.cList, row, pk, day, "today", isDeleted = false)
    val scanResult = getPreviousHBaseRow(table, pk)
    val keyPut = createKeyObject(pk, day)
    scanResult match {
      case Some(value) =>
        setVersion(put, value)
        val dayBefore = getPreviousDay(day)
        val update = new Put(value.getRow).addColumn(Bytes.toBytes("info"),
          Bytes.toBytes("dateTo"), Bytes.toBytes(dayBefore))
        putList.add(update)
      case None =>
        put.addColumn(Bytes.toBytes("info"),
          Bytes.toBytes("version"), Bytes.toBytes("1"))
    }
    putList.add(0, put)
    putList.add(1, keyPut)
  }

  def manageEditedRow(mc: MergeController, day: String, row: Row, table: Table, putList: util.ArrayList[Put]): Unit = {
    val pk = assembleRowKey(mc.keyList, row, "today")
    val put = createPutObject(mc.cList, row, pk, day, "today", isDeleted = false)
    val scanResult = getPreviousHBaseRow(table, pk)
    setVersion(put, scanResult.get)
    val dayBefore = getPreviousDay(day)
    val update = new Put(scanResult.get.getRow).addColumn(Bytes.toBytes("info"),
      Bytes.toBytes("dateTo"), Bytes.toBytes(dayBefore))
    val keyPut = createKeyObject(pk, day)
    putList.add(put)
    putList.add(keyPut)
    putList.add(update)
  }

  def manageRemovedRow(mc: MergeController, day: String, row: Row, table: Table, putList: util.ArrayList[Put]): Unit = {
    val pk = assembleRowKey(mc.keyList, row, "yesterday")
    val put = createPutObject(mc.cList, row, pk, day, "yesterday", isDeleted = true)
    val scanResult = getPreviousHBaseRow(table, pk)
    setVersion(put, scanResult.get)
    val dayBefore = getPreviousDay(day)
    val update = new Put(scanResult.get.getRow).
      addColumn(Bytes.toBytes("info"),
        Bytes.toBytes("dateTo"), Bytes.toBytes(dayBefore))
    val keyPut = HBaseUtils.createKeyObject(pk, day)
    putList.add(put)
    putList.add(keyPut)
    putList.add(update)
  }

  def assembleRowKey(keyList: Array[String], row: Row, version: String): String ={
    keyList.map(cName => row.get(row.fieldIndex(version + "_" + cName)).toString).mkString("|")
  }

  def manageNewHBaseTable(mc: MergeController): Unit = {
    val hbaseAdmin = ConnectionFactory.createConnection(mc.conf).getAdmin
    if (!hbaseAdmin.tableExists(TableName.valueOf(Bytes.toBytes(mc.hbaseTableName)))) {
      val ht = TableDescriptorBuilder.newBuilder(TableName.valueOf(Bytes.toBytes(mc.hbaseTableName))).
        setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("data".getBytes).
          setCompressionType(Compression.Algorithm.SNAPPY).
          setDataBlockEncoding(DataBlockEncoding.FAST_DIFF).build()).
        setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes).
          setCompressionType(Compression.Algorithm.SNAPPY).
          setDataBlockEncoding(DataBlockEncoding.FAST_DIFF).build()).
        setCompactionEnabled(true).build()

      val range = Integer.parseInt("fff" , 16)
      val n = 10  //number of regions
      val splitPoints = new Array[Array[Byte]](n)
      for(i <- 0 until n - 1){
        splitPoints(i) = ("pk_" + ((range/n) * (i + 1)).toHexString.reverse.padTo(3, "0").reverse.mkString("")).getBytes()
      }
      splitPoints(n - 1) = "sk_000".getBytes()
      hbaseAdmin.createTable(ht, splitPoints)
    }
  }

  def rollBack(mc: MergeController, spark: SparkSession, errorDate: String): Unit = {
    val pk = s"pk_${getHashValue(errorDate)}_$errorDate"
    val filtersList = new FilterList()
    filtersList.addFilter(new FirstKeyOnlyFilter())
    filtersList.addFilter(new KeyOnlyFilter())
    val scan = new Scan().withStartRow(Bytes.toBytes(pk)).
                          withStopRow(Bytes.unsignedCopyAndIncrement(Bytes.toBytes(pk))).setFilter(filtersList)
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, mc.hbaseTableName)
    hbaseConf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan))
    val hTableRDD = spark.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
                    classOf[ImmutableBytesWritable], classOf[Result])
    hTableRDD.foreachPartition { w =>
      val hbaseConf = HBaseConfiguration.create()
      val connection = ConnectionFactory.createConnection(hbaseConf)
      val table = connection.getTable(TableName.valueOf(Bytes.toBytes(mc.hbaseTableName)))
      w.foreach { rddRow =>
        val row = rddRow._2
        val errorPk = row.getRow
        val regex = "pk_.{3}_.{8}_".r
        val rowKey = regex.replaceFirstIn(Bytes.toString(errorPk), "")
        val dayBefore = getPreviousDay(errorDate)
        val scan = new Scan().setLimit(1).setReversed(true).
                              withStartRow(Bytes.toBytes(s"sk_${getHashValue(rowKey)}_${rowKey}_$dayBefore")).
                              withStopRow(Bytes.toBytes(s"sk_${getHashValue(rowKey)}_${rowKey}_00000000"))
        val scanResult = Option(table.getScanner(scan).next())
        scanResult match {
          case Some(scanValue) =>
            val previousPk = CellUtil.cloneValue(scanValue.getColumnLatestCell(Bytes.toBytes("info"),
              Bytes.toBytes("mergeKey")))
            val reopenRow = new Put(previousPk)
            reopenRow.addColumn(Bytes.toBytes("info"),
              Bytes.toBytes("dateTo"), Bytes.toBytes("99999999"))
            table.put(reopenRow)
          case None =>
        }
        val deleteSk = new Delete(Bytes.toBytes(s"sk_${getHashValue(rowKey)}_${rowKey}_$errorDate"))
        table.delete(deleteSk)
        val deletePk = new Delete(errorPk)
        table.delete(deletePk)
      }
    }
  }

  def doBulkLoad(mc: MergeController, kvRDD: RDD[(String, String, String, String)]): Unit = {
    mc.conf.set(HConstants.TEMPORARY_FS_DIRECTORY_KEY, MergeConfig.hBaseStagingDir)
    val job = Job.getInstance(mc.conf, "BulkLoad")
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])

    val tName = TableName.valueOf(mc.hbaseTableName)
    val connection = ConnectionFactory.createConnection(mc.conf)
    val table = connection.getTable(tName)
    val regionLocator = connection.getRegionLocator(tName)
    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

    val filePath = MergeConfig.hBaseTmpDirectory + mc.database + "-" + table + System.currentTimeMillis()
    kvRDD.sortBy(r => r).map(r =>
      (new ImmutableBytesWritable(Bytes.toBytes(r._1)),
        new KeyValue(Bytes.toBytes(r._1), Bytes.toBytes(r._2), Bytes.toBytes(r._3), Bytes.toBytes(r._4))))
      .saveAsNewAPIHadoopFile(filePath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2],
        job.getConfiguration)
    val bulkLoad = new LoadIncrementalHFiles(mc.conf)
    bulkLoad.doBulkLoad(new Path(filePath), connection.getAdmin, table, regionLocator)
  }

  def bulkLoadFirstDate(mc: MergeController, day: String, hbasePuts: Dataset[Row]): Unit = {
    val kvRDD = hbasePuts.rdd.coalesce(32).flatMap { row =>
      val pk = assembleRowKey(mc.keyList, row, "today")
      val rowKey = "pk_" + getHashValue(day) + "_" + day + "_" + pk
      val listBuffer = new ListBuffer[(String, String, String, String)]
      mc.cList.sorted.foreach(cName =>
        if (!row.isNullAt(row.fieldIndex("today_" + cName)) && row.get(row.fieldIndex("today_" + cName)).
          toString.nonEmpty) {
          listBuffer.append((rowKey, "data", cName, row.get(row.fieldIndex("today_" + cName)).toString))
        }
      )
      listBuffer.append((rowKey, "info" +
        "", "dateFrom", day))
      listBuffer.append((rowKey, "info" +
        "", "dateTo", "99999999"))
      listBuffer.append((rowKey, "info" +
        "", "deleteFlag", "0"))
      listBuffer.append((rowKey, "info" +
        "", "version", "1"))
      listBuffer.append((rowKey, "info" +
        "", "uniqueId", pk + "_" + day))
      listBuffer.append((rowKey, "info" +
        "", "pk", pk))

      val sk = "sk_" + getHashValue(pk) + "_" + pk + "_" + day
      listBuffer.append((sk, "info" +
        "", "mergeKey", rowKey))
      listBuffer.toList
    }
    doBulkLoad(mc, kvRDD)
  }

  def singlePutInsertion(mc: MergeController, day: String, hbasePuts: Dataset[Row]): Unit = {
    hbasePuts.foreachPartition { w =>
      val hbaseConf = HBaseConfiguration.create()
      val connection = ConnectionFactory.createConnection(hbaseConf)
      val table = connection.getTable(TableName.valueOf(Bytes.toBytes(mc.hbaseTableName)))
      w.foreach { row =>
        val putList = new util.ArrayList[Put]()
        row.getString(row.fieldIndex("status")) match {
          case "NEW" =>
            manageNewRow(mc, day, row, table, putList)
          case "EDIT" =>
            manageEditedRow(mc, day, row, table, putList)
          case "REMOVE" =>
            manageRemovedRow(mc, day, row, table, putList)
        }
        table.put(putList)
      }
      table.close()
      connection.close()
    }
  }
}
