/**
 * Copyright (c) 2011-2015 All Rights Reserved.
 */
package com.eaphayim.summarystatistic

import org.apache.spark.util.IntParam
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.PairDStreamFunctions
import org.apache.spark.Logging
import org.apache.spark.streaming.flume.SparkFlumeEvent
import org.apache.spark.SparkConf
import java.text.{ SimpleDateFormat => JSimpleDateFormat }
import java.util.{ Date => JDate, UUID => JUUID }
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._
import org.apache.hive.hcatalog.streaming.HiveEndPoint
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter
import scala.util.Random

/**
 *
 *
 * @author 464281
 * @version $Id: HessianClientAnalysis.java 2015年1月8日 上午11:44:39 $
 */
object HessianClientAnalysis extends Logging {

  def main(args: Array[String]) {
    val sc = new SparkConf
    sc.setAppName("HessianClientAnalysis")
    
    val host = "master01.bigdata.sfp.com"
    //val port = sc.get("hessian.client.stream.port").toInt
    //val batchInterval = Milliseconds(sc.get("hessian.client.stream.batch.interval", "2000").toInt)
    val port = 9090
    val batchInterval = Milliseconds(2000)
    
    val ssc = new StreamingContext(sc, batchInterval)
    val stream = FlumeUtils.createStream(ssc, host, port, StorageLevel.MEMORY_ONLY_SER_2)
//    val that = FlumeUtils.createStream(ssc, host, port, StorageLevel.MEMORY_ONLY_SER_2)
    
//    stream.union(that).

    // 去重
    val uniqueStream = stream.map(e => makeUniqueID(e)).reduceByKey((v1, v2) => v1)
    val analysisStream = uniqueStream.map(e => toItem(e._2))

    // 汇总时间以及调用次数
    val statisticsStream = analysisStream.reduceByKey { (v1, v2) =>
      (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3)
    }
    val saveStream = statisticsStream.map(e => ((e._1._1,e._1._3),e))
    val dataStream = saveStream.groupByKey()

//    statisticsStream.foreach { r => makeStr(r) }
    
    dataStream.foreach{rdd => saveData(rdd)}
    

    ssc.start()
    ssc.awaitTermination()
  }
  
  def makeStr(rdd: RDD[((String, String, String, String), (Int, Int, Int))]): Unit = {
    val arr = rdd.collect
    arr.foreach(e =>
        logInfo("****Analysis result:"+e._1._1 + " " + e._1._2 + " " + e._1._3 + " " + e._1._4 + " " + e._2._1 + " " + e._2._2 + " " + e._2._3))
  }

  def getUniqueId(e: SparkFlumeEvent): String = {
    val uniqueId = e.event.getHeaders().get("uniqueId")
    if (uniqueId == null || uniqueId.length == 0) {
      createUniqueId()
    } else {
      uniqueId.toString
    }
  }
  
  /**
   * save data
   */
  def saveData(rdd: RDD[((String, String), Iterable[((String, String, String, String), (Int, Int, Int))])]): Unit = {
    rdd.foreach(data => saveData2Hive(data))
  }
  /**
   * save data to hive
   * hive table must has a unused column 
   */
  def saveData2Hive(data: ((String, String), Iterable[((String, String, String, String), (Int, Int, Int))])): Unit = {
    val dbName = "default"
    val tblName = "hessianclientanalysis"
    //desc是无用字段，但不能为空值，解决hive shell查询时会少一列的情况
    val fieldNames = List("id", "date", "time", "appname", "methodname", "costs", "success", "total","desc").toArray
    val serdeClass = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    val partitionVals = List(data._1._2,data._1._1).asJava
    val hiveEP = new HiveEndPoint("thrift://master02.bigdata.sfp.com:9083", dbName, tblName, partitionVals)
    val connection = hiveEP.newConnection(true)
    val writer = new DelimitedInputWriter(fieldNames, ",", hiveEP)
    val txnBatch = connection.fetchTransactionBatch(10, writer)
    val results = data._2
    
    txnBatch.beginNextTransaction()
    for(result <- results) {
      val str = createUniqueId + ","+ result._1._1+","+ result._1._2+","+ result._1._3+","+ result._1._4+","+result._2._1+","+result._2._2+","+ result._2._3+","+"desc|"+ result._2._3
      txnBatch.write(str.getBytes())
    }
    txnBatch.commit();
    
    txnBatch.close()
    connection.close()
  }

  def makeUniqueID(event:SparkFlumeEvent):(String,SparkFlumeEvent) = {
    logInfo("**********Recieve event********")
    (getUniqueId(event), event)
  }
  def createUniqueId(): String = {
    val df = new JSimpleDateFormat("yyyyMMddHHmmss")
    df.format(new JDate) + JUUID.randomUUID().toString
  }

  def toItem(e: SparkFlumeEvent): ((String, String, String, String), (Int, Int, Int)) = {
    val event = e.event
    val failWord = "illegal"
    var appName = failWord
    try {
      val header = event.getHeaders().asScala
      val headerMap = collection.mutable.Map[String,String]()
      for((key,value) <- header) {
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

      val methodName = items(7).trim
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
}