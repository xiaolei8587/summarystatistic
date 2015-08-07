/**
 *
 */
package com.eaphayim.summarystatistic

import java.text.{ SimpleDateFormat => JSimpleDateFormat }
import java.util.{ Date => JDate }
import java.util.{ UUID => JUUID }
import scala.collection.JavaConverters.mapAsScalaMapConverter
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.flume.SparkFlumeEvent
import com.datastax.spark.connector._
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import com.datastax.driver.core.Cluster
import java.util.HashSet
import java.util.Date
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream
import java.util.concurrent.Executors
import java.util.concurrent.CountDownLatch
import com.datastax.driver.core.PreparedStatement
import java.util.concurrent.FutureTask
import java.util.concurrent.Callable
import java.util.concurrent.Future
import org.apache.spark.streaming.Seconds

/**
 * 摘要统计信息存储到cassandra
 *
 * @author 641158
 * @version 2015-01-15
 */
object HessianClientAnalysisWithCassandra extends Logging {

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
    //saprk.hessianclientanalysis 实时数据
    //saprk.analysissummary_date 每天的汇总数据  
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$tablename (id VARCHAR PRIMARY KEY, appname VARCHAR , date VARCHAR, time VARCHAR,  methodname VARCHAR, costs INT, success INT, total INT )")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$summaryDate (appname VARCHAR , date VARCHAR, methodname VARCHAR, costs INT, success INT, total INT, PRIMARY KEY(appname,date,methodname) )")
    }

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

    //    val uniqueStream = totalStream.map(e => makeUniqueID(e)).reduceByKey((v1, v2) => v1)
    val analysisStream = totalStream.map(e => toItem(e))

    // 汇总时间以及调用次数
    val statisticsStream = analysisStream.reduceByKey { (v1, v2) =>
      (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3)
    }
    //(date, time, appName, methodName), (iElapsedTime, iIsSuccess, total)
    val saveStream = statisticsStream.map(e => new LogAnalysis(e._1._3, e._1._1, e._1._2, e._1._4, e._2._1, e._2._2, e._2._3))
    //    statisticsStream.cache
    statisticsStream.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    //save detail
    saveStream.saveToCassandra(keyspace, tablename, SomeColumns("id", "appname", "date", "methodname", "time", "costs", "success", "total"))
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

    /*val summaryDateStream = saveStream.map(obj => new LogAnalysisSummary(obj.appname, obj.date, obj.methodname, obj.costs, obj.success, obj.total))
    //    summaryDateStream.foreach(rdd => saveData(rdd))
    val summaryGroup = summaryDateStream.map(obj => ((obj.appname, obj.date, obj.methodname), obj))
    val dataStream = summaryGroup.groupByKey()
    val groupStream = dataStream.map(sumDatas)
    groupStream.foreach(rdd => saveData(rdd))*/

    val summaryDateStream = statisticsStream.map(transLogAnalysisSummary)
    //    summaryDateStream.checkpoint(slideInterval)
    //    val saveSummaryStream = summaryDateStream.updateStateByKey[LogAnalysisSummary](updateFunc).map(data => data._2)
    //    saveSummaryStream.saveToCassandra(keyspace, summaryDate, SomeColumns("appname", "date", "methodname", "costs", "success", "total"))
    val windowStream = summaryDateStream.reduceByKeyAndWindow((a: LogAnalysisSummary, b: LogAnalysisSummary) => (a.count(b)), (a: LogAnalysisSummary, b: LogAnalysisSummary) => (a.minus(b)), winInterval, slideInterval)
    val saveSummaryWindowStream = windowStream.updateStateByKey[LogAnalysisSummary](updateFunc).map(data => data._2)
    saveSummaryWindowStream.saveToCassandra(keyspace, summaryDate, SomeColumns("appname", "date", "methodname", "costs", "success", "total"))
    if (!"".equals(checkpointDirectory)) {
      ssc.checkpoint(checkpointDirectory)
    }

    ssc
  }

  /**
   * 查询汇总记录
   * @param appname
   * @param date
   * @param methodname
   */
  def getLogAnalysisSummary(data: LogAnalysisSummary): LogAnalysisSummary = {
    if (data == null) {
      return null
    }
    //    val beginTime = System.currentTimeMillis()
    val cluster = Cluster.builder().addContactPoint("10.79.11.222").addContactPoint("10.79.11.223").addContactPoint("10.79.11.226").addContactPoint("10.79.11.227").build
    val session = cluster.connect()
    val preStatement = session.prepare("select * from spark.analysissummary_date where appname = ? and date = ? and methodname = ?")
    val statement = preStatement.bind(data.appname, data.date, data.methodname)
    val reslut = session.execute(statement)
    var obj: LogAnalysisSummary = null
    if (reslut != null) {
      val it = reslut.iterator()
      if (it.hasNext()) {
        val row = it.next
        obj = new LogAnalysisSummary(row.getString("appname"), row.getString("date"), row.getString("methodname"), row.getInt("costs"), row.getInt("success"), row.getInt("total"))
      }
    }
    session.close()
    cluster.close()
    /*val endTime = System.currentTimeMillis()
    val costs = endTime - beginTime
    logInfo("********getLogAnalysisSummary costs:" + costs + "---" + data.appname + "," + data.date + "," + data.methodname)
    println("********getLogAnalysisSummary costs:" + costs + "---" + data.appname + "," + data.date + "," + data.methodname + "--" + obj)*/
    return obj
  }

  def transLogAnalysisSummary(obj: ((String, String, String, String), (Int, Int, Int))): (String, LogAnalysisSummary) = {
    val summary = new LogAnalysisSummary(obj._1._3, obj._1._1, obj._1._4, obj._2._1, obj._2._2, obj._2._3)
    (summary.getUniqueId, summary)
  }

  def sumDatas(obj: ((String, String, String), Iterable[com.eaphayim.summarystatistic.LogAnalysisSummary])): LogAnalysisSummary = {
    val results = obj._2
    var data: LogAnalysisSummary = null;
    for (result <- results) {
      if (data == null) {
        data = result
      } else {
        data = data.sum(result)
      }
    }
    data
  }

  /**
   * collect RDD
   * save data to cassandra
   * @param rdd
   */
  def saveData(rdd: RDD[LogAnalysisSummary]): Unit = {
    val arr = rdd.collect
    val size = arr.size
    if (size > 0) {
      var num = 100;
      while (size < num) {
        num = num / 2
      }
      val service = Executors.newFixedThreadPool(num)
      val cluster = Cluster.builder().addContactPoint("10.79.11.222").addContactPoint("10.79.11.223").addContactPoint("10.79.11.226").addContactPoint("10.79.11.227").build
      val session = cluster.connect()
      val preStatement = session.prepare("select * from spark.analysissummary_date where appname = ? and date = ? and methodname = ?")
      var idx = 0;
      val tasks = new Array[Future[String]](num)
      if (size >= num) {
        val n = size / num
        val r = size % num
        var tmpArr: Array[LogAnalysisSummary] = null

        while (idx < num) {
          if ((idx + 1) >= num) {
            tmpArr = new Array[LogAnalysisSummary](n + r)
            Array.copy(arr, idx * n, tmpArr, 0, n + r)
          } else {
            tmpArr = new Array[LogAnalysisSummary](n)
            Array.copy(arr, idx * n, tmpArr, 0, n)
          }
          val task = new DataHandler(tmpArr, session, preStatement)
          tasks(idx) = service.submit(task)
          idx += 1
        }
      }
      tasks.foreach(result => println(result.get()))
      service.shutdown();
      session.close()
      cluster.close()
    }
    //    rdd.foreachPartition(saveData2CassandraByPartition)
  }

  def saveData2CassandraByPartition(iterator: Iterator[LogAnalysisSummary]): Unit = {
    val beginTime = System.currentTimeMillis()
    val cluster = Cluster.builder().addContactPoint("10.79.11.222").addContactPoint("10.79.11.223").addContactPoint("10.79.11.226").addContactPoint("10.79.11.227").build
    val session = cluster.connect()
    val sb = new StringBuilder
    sb.append("BEGIN BATCH USING TIMESTAMP ")
    val comma = ","
    val comma_ex = "','"
    val quote = "'"
    val insertStr = "INSERT INTO spark.analysissummary_date(appname,date,methodname,costs,success,total) VALUES('"
    val preStatement = session.prepare("select * from spark.analysissummary_date where appname = ? and date = ? and methodname = ?")
    while (iterator.hasNext) {
      val data = iterator.next
      val statement = preStatement.bind(data.appname, data.date, data.methodname)
      val reslut = session.execute(statement)
      val rowDataMap = collection.mutable.Map[String, LogAnalysisSummary]()
      val it = reslut.iterator()
      while (it.hasNext) {
        val row = it.next
        val obj = new LogAnalysisSummary(row.getString("appname"), row.getString("date"), row.getString("methodname"), row.getInt("costs"), row.getInt("success"), row.getInt("total"))
        rowDataMap.put(obj.getUniqueId, obj)
      }
      val row = rowDataMap.getOrElse(data.getUniqueId, null)
      if (row == null) {
        sb.append(insertStr).append(data.appname).append(comma_ex).append(data.date).append(comma_ex).append(data.methodname).append(quote).append(comma).append(data.costs).append(comma).append(data.success).append(comma).append(data.total).append(");")
      } else {
        val obj = data.count(row)
        sb.append("UPDATE spark.analysissummary_date set costs =").append(obj.costs).append(",success=").append(obj.success).append(",total=").append(obj.total).append(" where appname='").append(obj.appname).append("' and date='").append(obj.date).append("' and methodname='").append(obj.methodname).append("';")
      }
    }
    sb.append(" APPLY BATCH")
    session.execute(sb.toString)
    val endTime = System.currentTimeMillis()
    val costs = endTime - beginTime
    logInfo("********sql execute costs:" + costs)
    session.close()
    cluster.close()
  }

  /**
   *
   */
  def saveData2CassandraWithBatch(d: ((String, String), Iterable[LogAnalysisSummary])): Unit = {
    val cluster = Cluster.builder().addContactPoint("10.79.11.222").addContactPoint("10.79.11.223").addContactPoint("10.79.11.226").addContactPoint("10.79.11.227").build
    val session = cluster.connect()
    val sb = new StringBuilder
    val iterator = d._2.iterator
    sb.append("BEGIN BATCH USING TIMESTAMP ")
    val comma = ","
    val comma_ex = "','"
    val quote = "'"
    val str = "INSERT INTO spark.analysissummary_date(appname,date,methodname,costs,success,total) VALUES('"
    val statement = session.prepare("select * from spark.analysissummary_date where appname = ? and date = ?").bind(d._1._1, d._1._2)
    val reslut = session.execute(statement)
    val rowDataMap = collection.mutable.Map[String, LogAnalysisSummary]()
    val it = reslut.iterator()
    while (it.hasNext) {
      val row = it.next
      val obj = new LogAnalysisSummary(row.getString("appname"), row.getString("date"), row.getString("methodname"), row.getInt("costs"), row.getInt("success"), row.getInt("total"))
      rowDataMap.put(obj.getUniqueId, obj)
    }
    while (iterator.hasNext) {
      val data = iterator.next
      val obj = data.count(rowDataMap.getOrElse(data.getUniqueId, null))
      sb.append(str).append(obj.appname).append(comma_ex).append(obj.date).append(comma_ex).append(obj.methodname).append(quote).append(comma).append(obj.costs).append(comma).append(obj.success).append(comma).append(obj.total).append(");")
    }
    sb.append(" APPLY BATCH")
    session.execute(sb.toString)
    sb.setLength(0)
    session.close()
    cluster.close()
  }
  /**
   *
   */
  def saveData2Cassandra(arr: Array[LogAnalysisSummary], session: com.datastax.driver.core.Session): Unit = {
    val beginTime = System.currentTimeMillis()
    val sb = new StringBuilder
    sb.append("BEGIN BATCH USING TIMESTAMP ")
    val comma = ","
    val comma_ex = "','"
    val quote = "'"
    val insertStr = "INSERT INTO spark.analysissummary_date(appname,date,methodname,costs,success,total) VALUES('"
    val preStatement = session.prepare("select * from spark.analysissummary_date where appname = ? and date = ? and methodname = ?")
    for (data <- arr) {
      val statement = preStatement.bind(data.appname, data.date, data.methodname)
      val reslut = session.execute(statement)
      val rowDataMap = collection.mutable.Map[String, LogAnalysisSummary]()
      val it = reslut.iterator()
      while (it.hasNext) {
        val row = it.next
        val obj = new LogAnalysisSummary(row.getString("appname"), row.getString("date"), row.getString("methodname"), row.getInt("costs"), row.getInt("success"), row.getInt("total"))
        rowDataMap.put(obj.getUniqueId, obj)
      }
      val row = rowDataMap.getOrElse(data.getUniqueId, null)
      if (row == null) {
        sb.append(insertStr).append(data.appname).append(comma_ex).append(data.date).append(comma_ex).append(data.methodname).append(quote).append(comma).append(data.costs).append(comma).append(data.success).append(comma).append(data.total).append(");")
      } else {
        val obj = data.count(row)
        sb.append("UPDATE spark.analysissummary_date set costs =").append(obj.costs).append(",success=").append(obj.success).append(",total=").append(obj.total).append(" where appname='").append(obj.appname).append("' and date='").append(obj.date).append("' and methodname='").append(obj.methodname).append("';")
      }
    }
    sb.append(" APPLY BATCH")
    session.execute(sb.toString)
    val endTime = System.currentTimeMillis()
    logInfo("********sql execute costs:" + (endTime - beginTime))
  }

  /**
   * 汇总数据，并保存到spark.analysissummary_date
   */
  def saveData2Cassandra(data: ((String, String), Iterable[LogAnalysisSummary])): Unit = {
    val cluster = Cluster.builder().addContactPoint("10.79.11.222").addContactPoint("10.79.11.223").addContactPoint("10.79.11.226").addContactPoint("10.79.11.227").build
    val session = cluster.connect()
    val statement = session.prepare("select * from spark.analysissummary_date where appname = ? and date = ?").bind(data._1._1, data._1._2)
    val reslut = session.execute(statement)
    val rowDataMap = collection.mutable.Map[String, LogAnalysisSummary]()
    val it = reslut.iterator()
    while (it.hasNext) {
      val row = it.next
      val obj = new LogAnalysisSummary(row.getString("appname"), row.getString("date"), row.getString("methodname"), row.getInt("costs"), row.getInt("success"), row.getInt("total"))
      rowDataMap.put(obj.getUniqueId, obj)
    }
    val sb = new StringBuilder
    sb.append("BEGIN BATCH USING TIMESTAMP ")
    val comma = ","
    val comma_ex = "','"
    val quote = "'"
    val str = "INSERT INTO spark.analysissummary_date(appname,date,methodname,costs,success,total) VALUES('"
    for (reslut <- data._2) {
      val obj = reslut.count(rowDataMap.getOrElse(reslut.getUniqueId, null))
      sb.append(str).append(obj.appname).append(comma_ex).append(obj.date).append(comma_ex).append(obj.methodname).append(quote).append(comma).append(obj.costs).append(comma).append(obj.success).append(comma).append(obj.total).append(");")
    }
    sb.append(" APPLY BATCH")
    session.execute(sb.toString)
    session.close()
    cluster.close()
  }

  /**
   * 转化SparkFlumeEvent
   */
  def toItem(e: SparkFlumeEvent): ((String, String, String, String), (Int, Int, Int)) = {
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
        return ((failWord, "", appName, "App name would not be null!"), (0, 0, 1))
      }

      val content = new String(event.getBody().array())
      val items = content.split(" ")
      if (items.length < 11) {
        return ((failWord, "", appName, "Length of event content split by ' ' is less than 11, content = " + content), (0, 0, 1))
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

      ((date, time, appName, methodName), (iElapsedTime, iIsSuccess, 1))

    } catch {
      case ex: Exception => {
        logError("Fail to convert event to item, event = " + event, ex);
        ((failWord, "", appName, "Fail to convert event to item, ex.message = " + ex.getMessage), (0, 0, 1))
      }
    }
  }

  def makeUniqueID(event: SparkFlumeEvent): (String, SparkFlumeEvent) = {
    (createUniqueId, event)
  }
  def createUniqueId(): String = {
    val df = new JSimpleDateFormat("yyyyMMddHHmmss")
    df.format(new JDate) + JUUID.randomUUID().toString
  }
}