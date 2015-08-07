/**
 *
 */
package com.eaphayim.summarystatistic

import org.apache.spark.streaming.Milliseconds
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

/**
 * @author 641158
 *
 */
object FlumeEventCount extends Logging {
  val cls = FlumeEventCount.getClass()
  def main(args: Array[String]): Unit = {
    val host = args(1).trim()
    val port = args(2).trim().toInt
    val batchInterval = Milliseconds(2000)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumeEventCount")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    // Create a flume stream
    val stream = FlumeUtils.createStream(ssc, host, port, StorageLevel.MEMORY_ONLY_SER_2)

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => makeStr(cnt)).foreach(x => show(x))
    //    stream.count().map(cnt => "Received " + cnt + " flume events." ).print

    ssc.start()
    ssc.awaitTermination()
  }

  def show(rdd: RDD[String]) = {
    val arr = rdd.collect
    arr.foreach(x => logInfo("****:" + x))
  }

  def makeStr(num: Long): String = {
    var str = "Received none"
    if (num > 0) {
      str = "Have received " + num + " flume events."
    }
    str
  }

}