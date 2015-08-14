package com.eaphayim.risk.datamarket

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.hbase.HTableDescriptor
import java.io.InputStreamReader
import com.mucfc.common.Utils

object AllDeviceView {

  def getKey(result: Result): String = {
    new String(result.getRow)
  }
  def getString(result: Result, famliy: String, column: String): String = {
    val ret = result.getValue(Bytes.toBytes(famliy), Bytes.toBytes(column))
    if (null == ret)
      null
    else
      new String(ret)
  }
  def getLong(result: Result, famliy: String, column: String): Long = {
    val ret = result.getValue(Bytes.toBytes(famliy), Bytes.toBytes(column))
    if (null == ret)
      0
    else
      Bytes.toLong(ret)
  }
  //CUSTRDD转换成HABASE.PUt
  def convertDev(triple: (String, (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, Long, String))) = {
    val p = new Put(Utils.getRowKey(triple._1, true))
    if (null != triple._2._1) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("APPNAME"), triple._2._1.getBytes)
    if (null != triple._2._2) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("APPTYPE"), triple._2._2.getBytes)
    if (null != triple._2._3) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("APPVERSION"), triple._2._3.getBytes)
    if (null != triple._2._4) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("MAC"), triple._2._4.getBytes)
    if (null != triple._2._5) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("IMEI"), triple._2._5.getBytes)
    if (null != triple._2._6) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("PHONEOPERATOR"), triple._2._6.getBytes)
    if (null != triple._2._7) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("PHONEMARKER"), triple._2._7.getBytes)
    if (null != triple._2._8) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("PHONEMODEL"), triple._2._8.getBytes)
    if (null != triple._2._9) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("COMPUTERHOST"), triple._2._9.getBytes)
    if (null != triple._2._10) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("OS"), triple._2._10.getBytes)
    if (null != triple._2._11) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("OSVERSION"), triple._2._11.getBytes)
    if (null != triple._2._12) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("RESOLUTION"), triple._2._12.getBytes)
    if (null != triple._2._13) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("FONT"), triple._2._13.getBytes)
    if (null != triple._2._14) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("FONTSIZE"), triple._2._14.getBytes)
    if (null != triple._2._15) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("BROWSER"), triple._2._15.getBytes)
    if (null != triple._2._16) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("BROWSERVERSION"), triple._2._16.getBytes)
    if (null != triple._2._17) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("SECURITYLEVEL"), triple._2._17.getBytes)
    if (null != triple._2._18) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("SRC_PRODUCTTYPE"), triple._2._18.getBytes)
    if (null != triple._2._19) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("USERAGENTCUST"), triple._2._19.getBytes)
    p.add(Bytes.toBytes("INFO"), Bytes.toBytes("TDATE_BEGINTIME"), Bytes.toBytes(triple._2._20))
    if (null != triple._2._21) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("IP"), Bytes.toBytes(triple._2._21))
    (new ImmutableBytesWritable, p)
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("AllDeviceView")
    val sc = new SparkContext(sparkConf)

    //用户信息RDD配置 
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "R_RISK_EVENT")

    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable("R_RISK_EVENT")) {
      val tableDesc = new HTableDescriptor("R_RISK_EVENT")
      admin.createTable(tableDesc)
    }

    //连接hbase，读表数据BASE:EVENTID
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).filter {
        case (key, result) =>
          val devId = getString(result, "DEV", "DEVICEID");
          null != devId && (!"".equals(devId.trim()))
      }
    val allDevRdd = hBaseRDD.mapPartitions({ iter =>
      for {
        (key, result) <- iter

      } yield (getString(result, "DEV", "DEVICEID"),
        (
          getString(result, "DEV", "APPNAME"),
          getString(result, "DEV", "APPTYPE"),
          getString(result, "DEV", "APPVERSION"),
          getString(result, "DEV", "MAC"),
          getString(result, "DEV", "IMEI"),
          getString(result, "DEV", "PHONEOPERATOR"),
          getString(result, "DEV", "PHONEMARKER"),
          getString(result, "DEV", "PHONEMODEL"),
          getString(result, "DEV", "COMPUTERHOST"),
          getString(result, "DEV", "OS"),
          getString(result, "DEV", "OSVERSION"),
          getString(result, "DEV", "RESOLUTION"),
          getString(result, "DEV", "FONT"),
          getString(result, "DEV", "FONTSIZE"),
          getString(result, "DEV", "BROWSER"),
          getString(result, "DEV", "BROWSERVERSION"),
          getString(result, "DEV", "SECURITYLEVEL"),
          getString(result, "SRC", "PRODUCTTYPE"),
          getString(result, "HTTP", "USERAGENTCUST"),
          getLong(result, "TDATE", "BEGINTIME"),
          getString(result, "LSB", "IP") + ":(" + getString(result, "LSB", "LONGITUDE") + "," + getString(result, "LSB", "LATITUDE") + "):" + getString(result, "LSB", "COUNTRY") + "." + getString(result, "LSB", "PROVINCE") + "." + getString(result, "LSB", "CITY") + "." + getString(result, "LSB", "STREET")))
    }, true)
    /*val allDevRdd = hBaseRDD.map {
      case (key, result) => (getString(result, "DEV", "DEVICEID"),
        (
          getString(result, "DEV", "APPNAME"),
          getString(result, "DEV", "APPTYPE"),
          getString(result, "DEV", "APPVERSION"),
          getString(result, "DEV", "MAC"),
          getString(result, "DEV", "IMEI"),
          getString(result, "DEV", "PHONEOPERATOR"),
          getString(result, "DEV", "PHONEMARKER"),
          getString(result, "DEV", "PHONEMODEL"),
          getString(result, "DEV", "COMPUTERHOST"),
          getString(result, "DEV", "OS"),
          getString(result, "DEV", "OSVERSION"),
          getString(result, "DEV", "RESOLUTION"),
          getString(result, "DEV", "FONT"),
          getString(result, "DEV", "FONTSIZE"),
          getString(result, "DEV", "BROWSER"),
          getString(result, "DEV", "BROWSERVERSION"),
          getString(result, "DEV", "SECURITYLEVEL"),
          getString(result, "SRC", "PRODUCTTYPE"),
          getString(result, "HTTP", "USERAGENTCUST"),
          getLong(result, "TDATE", "BEGINTIME"),
          getString(result, "LSB", "IP")+":("+ getString(result, "LSB", "LONGITUDE")+","+getString(result, "LSB", "LATITUDE")+"):"+getString(result, "LSB", "COUNTRY") + "." + getString(result, "LSB", "PROVINCE") + "." + getString(result, "LSB", "CITY") + "." + getString(result, "LSB", "STREET")))
    }*/
    //插入数据到hbase
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "U_ALLDEVICE_INFO_NEW")
    allDevRdd.mapPartitions({
      iter =>
        for {
          (key, (appname, apptype, str1, str2, str3, str4, str5, str6, str7, str8, str9, str10, str11, str12, str13, str14, str15, str16, str17, time, ip)) <- iter

        } yield {
          val p = new Put(Utils.getRowKey(key, true))
          if (null != appname) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("APPNAME"), appname.getBytes)
          if (null != apptype) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("APPTYPE"), apptype.getBytes)
          if (null != str1) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("APPVERSION"), str1.getBytes)
          if (null != str2) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("MAC"), str2.getBytes)
          if (null != str3) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("IMEI"), str3.getBytes)
          if (null != str4) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("PHONEOPERATOR"), str4.getBytes)
          if (null != str5) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("PHONEMARKER"), str5.getBytes)
          if (null != str6) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("PHONEMODEL"), str6.getBytes)
          if (null != str7) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("COMPUTERHOST"), str7.getBytes)
          if (null != str8) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("OS"), str8.getBytes)
          if (null != str9) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("OSVERSION"), str9.getBytes)
          if (null != str10) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("RESOLUTION"), str10.getBytes)
          if (null != str11) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("FONT"), str11.getBytes)
          if (null != str12) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("FONTSIZE"), str12.getBytes)
          if (null != str13) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("BROWSER"), str13.getBytes)
          if (null != str14) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("BROWSERVERSION"), str14.getBytes)
          if (null != str15) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("SECURITYLEVEL"), str15.getBytes)
          if (null != str16) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("SRC_PRODUCTTYPE"), str16.getBytes)
          if (null != str17) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("USERAGENTCUST"), str17.getBytes)
          p.add(Bytes.toBytes("INFO"), Bytes.toBytes("TDATE_BEGINTIME"), Bytes.toBytes(time))
          if (null != ip) p.add(Bytes.toBytes("INFO"), Bytes.toBytes("IP"), Bytes.toBytes(ip))
          (new ImmutableBytesWritable, p)
        }
    }, true).saveAsHadoopDataset(jobConfig)
    //    allDevRdd.map(convertDev).saveAsHadoopDataset(jobConfig)
    sc.stop();

  }

}