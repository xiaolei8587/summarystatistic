/**
 *
 */
package com.eaphayim.summarystatistic

import org.apache.spark.Logging
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.flume.SparkFlumeEvent
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.storage.StorageLevel
import scala.collection.JavaConverters.mapAsScalaMapConverter
import org.apache.spark.rdd.RDD
import org.apache.phoenix.spark._
import org.apache.spark.SparkContext

/**
 * @author Eapha Yim
 *
 */
object HessianClientAnalysisWithHBase extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      throw new SparkException("args need params:host port batchInterval checkpointDirectory windowInterval")
    }
    val pattern = "\\D".r
    val hostStr = args(0).trim
    if (hostStr.length() < 1) {
      throw new SparkException("host cannot be blank!")
    }
    val portStr = pattern.replaceAllIn(args(1), "").trim
    val interval = pattern.replaceAllIn(args(2), "").trim
    if (portStr.length() < 1) {
      throw new SparkException("port{" + args(1) + "} is illegal !")
    }
    if (interval.length() < 1) {
      throw new SparkException("batchInterval(ms){" + args(2) + "} is illegal !")
    }
    val checkpointDirectory = pattern.replaceAllIn(args(3), "").trim
    if (!checkpointDirectory.startsWith("hdfs://")) {
      throw new SparkException("checkpointDirectory[" + args(3) + "] is illegal, must be a hdfs directory !")
    }
    val windowInterval = pattern.replaceAllIn(args(4), "").trim
    if (windowInterval.length() < 1) {
      throw new SparkException("windowInterval(ms){" + args(4) + "} is illegal !")
    }
    val ssc = StreamingContext.getOrCreate(checkpointDirectory,
      () => {
        createContext(hostStr, portStr, interval, checkpointDirectory, windowInterval)
      })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * create the context
   * 
   */
  def createContext(hostStr: String, portStr: String, interval: String, checkpointDirectory: String, windowInterval: String) = {
    val conf = new SparkConf()
    //    conf.registerKryoClasses(Array(classOf[LogAnalysisSummary], classOf[LogAnalysis]))
    val keyspace = "spark"
    val tablename = "hessianclientanalysis"
    val summaryDate = "analysissummary_date"
    val port = portStr.toInt
    val batchInterval = Milliseconds(interval.toInt)
    val winInterval = Milliseconds(windowInterval.toInt)
    val slideInterval = Milliseconds(interval.toInt + windowInterval.toInt)
    //create table
    //CREATE TABLE IF NOT EXISTS hessianclientanalysis (id VARCHAR PRIMARY KEY, appname VARCHAR , date VARCHAR, time VARCHAR,  methodname VARCHAR, costs INT, success INT, total INT )
    //CREATE TABLE IF NOT EXISTS analysissummary_date (appname VARCHAR , date VARCHAR, methodname VARCHAR, costs INT, success INT, total INT, PRIMARY KEY(appname,date,methodname))
    val hosts = hostStr.split(",")
    val ssc = new StreamingContext(conf, batchInterval)

    var totalStream: DStream[SparkFlumeEvent] = null
    for (host <- hosts) {
      //[MODIFIED][20150122]flume送过来的数据太快，spark处理不过来，如果只放内存中会导致后面RDD计算时block已被删掉，以致整个application失败，故StorageLevel 最好是用MEMORY_AND_DISK_SER_2
      val stream = FlumeUtils.createStream(ssc, host, port, StorageLevel.MEMORY_AND_DISK_SER_2)
      if (totalStream == null) {
        totalStream = stream
      } else {
        if (hosts.length > 1) {
          totalStream = totalStream.union(stream)
        }
      }
    }

    val analysisStream = totalStream.map(e => toItem(e))

    // 汇总时间以及调用次数
    val statisticsStream = analysisStream.reduceByKey { (v1, v2) =>
      (v1.count(v2))
    }
    val saveStream = statisticsStream.map(e => e._2.getLog())
    //    statisticsStream.cache
    statisticsStream.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    //save detail
    saveStream.foreachRDD(rdd => saveLogAnalysisToHBaseWithPhoenix(rdd))
    //save Summary
    lazy val updateFunc = (values: Seq[LogAnalysisSummary], state: Option[LogAnalysisSummary]) => {
      var summary: LogAnalysisSummary = null
      for (obj <- values) {
        if (summary == null) {
          summary = obj
        } else {
          summary = summary.count(obj)
        }
      }
      //不查数据库，job一开始肯定是没有数据的，但如果是重跑的话则是需要查询数据库的，为了效率暂时屏蔽掉下面这行代码，直接用null代替
      //      val previousCount = state.getOrElse(getLogAnalysisSummary(summary))
      val previousCount = state.getOrElse(null)
      if (summary != null) {
        Some(summary.count(previousCount))
      } else if (previousCount != null) {
        Some(previousCount.count(summary))
      } else {
        Some(null)
      }
    }

    val summaryDateStream = statisticsStream.map(transLogAnalysisSummary)
    //    summaryDateStream.checkpoint(slideInterval)
    //    val saveSummaryStream = summaryDateStream.updateStateByKey[LogAnalysisSummary](updateFunc).map(data => data._2)
    //    saveSummaryStream.saveToCassandra(keyspace, summaryDate, SomeColumns("appname", "date", "methodname", "costs", "success", "total"))
    val windowStream = summaryDateStream.reduceByKeyAndWindow((a: LogAnalysisSummary, b: LogAnalysisSummary) => (a.count(b)), (a: LogAnalysisSummary, b: LogAnalysisSummary) => (a.minus(b)), winInterval, slideInterval)
    val saveSummaryWindowStream = windowStream.updateStateByKey[LogAnalysisSummary](updateFunc).map(data => data._2)
    //    saveSummaryWindowStream.saveToCassandra(keyspace, summaryDate, SomeColumns("appname", "date", "methodname", "costs", "success", "total"))
    if (!"".equals(checkpointDirectory)) {
      ssc.checkpoint(checkpointDirectory)
    }

    ssc
  }

  /**
   * phoenix 部署在A-HDS111
   */
  def saveLogAnalysisToHBaseWithPhoenix(rdd: RDD[(String, String, String, String, String, Int, Int, Int)]): Unit = {

    //    val hConf = new HBaseConfiguration()
    //  val hTable = new HTable(hConf, "table")
    //  val thePut = new Put(Bytes.toBytes(row(0)))
    //  thePut.add(Bytes.toBytes("cf"), Bytes.toBytes(row(0)), Bytes.toBytes(row(0)))
    //  hTable.put(thePut)
    val sc = rdd.context
    rdd.saveToPhoenix(
        "hessianclientanalysis",
        Seq("id", "appname", "date", "time", "methodname", "costs", "success", "total"),
        zkUrl = Some("A-HDS111:2181"))
  }

  def toItem(e: SparkFlumeEvent): (String, LogAnalysis) = {
    val event = e.event
    val failWord = "illegal"
    var appName = failWord
    try {
      val header = event.getHeaders().asScala
      val headerMap = collection.mutable.Map[String, String]()
      for ((key, value) <- header) {
        headerMap += (key.toString() -> value.toString())
      }
      appName = headerMap.getOrElse("appName", failWord)
      if ("illegal" == appName) {
        //        return ((failWord, "", appName, "App name would not be null!"), (0, 0, 1))
        return null
      }

      val content = new String(event.getBody().array())
      val items = content.split(" ")
      if (items.length < 11) {
        //        return ((failWord, "", appName, "Length of event content split by ' ' is less than 11, content = " + content), (0, 0, 1))
        return null
      }

      // 获取日期时间等统计数据
      val date = items(0).trim
      val tTime = items(1).trim
      val time = tTime.substring(0, tTime.length - 4)

      val _methodName = items(7).trim
      val methodName = _methodName.substring(0, _methodName.length() - 1)
      val remoteName = items(8).trim
      val remote = remoteName.substring(0, remoteName.length() - 1)
      val isSuccess = items(10).trim
      val iIsSuccess = if (isSuccess == "Y") 1 else 0

      val elapsedTime = items(9).trim
      val pattern = "\\D".r
      val iElapsedTime = pattern.replaceAllIn(elapsedTime, "").trim().toInt

      //      ((date, time, appName, methodName), (iElapsedTime, iIsSuccess, 1))
      val log = new LogAnalysis(appName, date, time, methodName, iElapsedTime, iIsSuccess, 1)
      (log.getUniqueId(), log)

    } catch {
      case ex: Exception => {
        logError("Fail to convert event to item, event = " + event, ex)
        //        ((failWord, "", appName, "Fail to convert event to item, ex.message = " + ex.getMessage), (0, 0, 1))
        return null
      }
    }
  }

  /**
   *
   */
  def transLogAnalysisSummary(obj: (String, LogAnalysis)): (String, LogAnalysisSummary) = {
    val summary = new LogAnalysisSummary(obj._2.appname, obj._2.date, obj._2.methodname, obj._2.costs, obj._2.success, obj._2.total)
    (summary.getUniqueId, summary)
  }
}