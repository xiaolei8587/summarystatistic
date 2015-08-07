/**
 *
 */
package com.eaphayim.summarystatistic

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.hive.hcatalog.streaming.HiveEndPoint
import org.apache.hive.hcatalog.streaming.StreamingConnection
import scala.collection.JavaConverters._
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter
import org.apache.spark.SparkContext
import scala.util.Random

/**
 * @author 641158
 *
 */
object HiveStreamingInsert extends Logging {

  def main(args: Array[String]): Unit = {
    logInfo("*********HiveStreamingInsert*************")
    val sparkConf = new SparkConf().setAppName("HiveStreamingInsert")
    val sc = new SparkContext(sparkConf)
    val dbName = "default"
    val tblName = "alerts"
    val fieldNames = List("id", "msg").toArray
    val serdeClass = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
    val partitionVals = List("Asia", "India").asJava
    logInfo("*********HiveStreamingInsert fieldNames*************"+fieldNames.toString())
    val hiveEP = new HiveEndPoint("thrift://master02.bigdata.sfp.com:9083", dbName, tblName, partitionVals)
    val connection = hiveEP.newConnection(true)
    val writer = new DelimitedInputWriter(fieldNames, ",", hiveEP)
    val txnBatch = connection.fetchTransactionBatch(2, writer)
    logInfo("*********HiveStreamingInsert Batch 1 - First TXN*************")
    ///// Batch 1 - First TXN
    txnBatch.beginNextTransaction()
    txnBatch.write((Random.nextInt+",Hello streaming").getBytes())
    txnBatch.write((Random.nextInt+",Welcome to streaming").getBytes())
    txnBatch.commit();
    logInfo("*********HiveStreamingInsert Batch 1 commit*************")
    if (txnBatch.remainingTransactions() > 0) {
      ///// Batch 1 - Second TXN
      txnBatch.beginNextTransaction();
      txnBatch.write((Random.nextInt+",Roshan Naik").getBytes())
      txnBatch.commit()
    }
    txnBatch.close()
    connection.close()
    sc.stop
  }

}