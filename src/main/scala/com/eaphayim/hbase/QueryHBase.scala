/**
 *
 */
package com.eaphayim.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

/**
 * @author Eapha Yim
 * @time 2015年7月10日
 */
object QueryHBase extends Logging {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    // Other options for configuring scan behavior are available. More information available at
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    val table = args(0)
    conf.set(TableInputFormat.INPUT_TABLE, table)

    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(args(0))) {
      val tableDesc = new HTableDescriptor(args(0))
      admin.createTable(tableDesc)
    }

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val num = hBaseRDD.count()
    println(s"Table:$table, size:$num")
    
    /*******************new hadoop api*************************/
   /* val job = new Job()
    job.setOutputFormatClass(classOf[TextOutputFormat[LongWritable,Text]])
    job.getConfiguration().set("mapred.output.compress", "true")
    job.getConfiguration().set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec")
    val textFile = sc.newAPIHadoopFile(args(0), classOf[LzoTextInputFormat],classOf[LongWritable], classOf[Text],job.getConfiguration())
    textFile.saveAsNewAPIHadoopFile(args(1), classOf[LongWritable], classOf[Text],classOf[TextOutputFormat[LongWritable,Text]],job.getConfiguration())
     
     
     
    *//*******************textFile*************************//*
    val textFile = sc.textFile(args(0), 1)
    textFile.saveAsTextFile(args(1),  classOf[LzopCodec])
    */
    sc.stop()
  }

}