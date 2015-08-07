/**
 *
 */
package com.eaphayim.risk.datamarket

import org.apache.spark.Logging
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import scala.collection.immutable.Map
import scala.collection.mutable.HashMap
import org.apache.spark.mllib.clustering.KMeansModel
import scopt.OptionParser
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

/**
 * @author Eapha Yim
 * @date 2015年7月29日
 */
object GPSKMeans extends Logging {

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
  object InitializationMode extends Enumeration {
    type InitializationMode = Value
    val Random, Parallel = Value
  }
  import InitializationMode._
  case class Params(
      k: Int = -1,
      numIterations: Int = 10,
      initializationMode: InitializationMode = Parallel)
  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("DenseKMeans") {
      head("GPSKMeans: ")
      opt[Int]('k', "k")
        .required()
        .text(s"number of clusters, required")
        .action((x, c) => c.copy(k = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[String]("initMode")
        .text(s"initialization mode (${InitializationMode.values.mkString(",")}), " +
        s"default: ${defaultParams.initializationMode}")
        .action((x, c) => c.copy(initializationMode = InitializationMode.withName(x)))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
    
  }
  
  def run(params: Params) {
    val sparkConf = new SparkConf().setAppName("GPSKMeans-Test")
    val maxK = params.k
    val initMode = params.initializationMode match {
      case Random => KMeans.RANDOM
      case Parallel => KMeans.K_MEANS_PARALLEL
    }
    val gpsPath = "/yyh/gpsData"
    val distinctGPSPath = "/yyh/distinctGPS"
    val predictedGPSPath = "/yyh/predictedGPS"
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "PRMI_RISK_EVENT")
    val dstPath = new Path(gpsPath)
    val hdfs = FileSystem.get(conf)
    if (hdfs.exists(dstPath)) {
      hdfs.delete(dstPath, true)
      hdfs.delete(new Path(distinctGPSPath), true)
      hdfs.delete(new Path(predictedGPSPath), true)
    }
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).map {
        case (key, result) => (
          getString(result, "INFO", "USER_MOBILE_NO"),
          getString(result, "INFO", "CITY_ID"),
          getString(result, "INFO", "FM_LONGITUDE"),
          getString(result, "INFO", "FM_LATITUDE"))
      }.filter { x =>
        null != x._1 && (!"".equals(x._1.trim())) &&
          null != x._3 && (!"".equals(x._3.trim())) &&
          null != x._4 && (!"".equals(x._4.trim()))
      }.map { x =>
        try {
          (x._1.toDouble, x._3.toDouble, x._4.toDouble, x._2)
        } catch {
          //case ex:Exception=>ex.printStackTrace()
          case _: Exception => print(_)
          (0.toDouble, 0.toDouble,0.toDouble,0)
        }
      }.filter(x => x._1 != 0.toDouble)
//  }.filter(x => x._1 != 0.toDouble && x._2 > 0 && x._3 > 0)
    hBaseRDD.map(x => x._2+" "+x._3).distinct().repartition(1).saveAsTextFile(distinctGPSPath)
    val data =hBaseRDD.map { x => x._1+" "+x._2+" "+x._3 +" "+x._4}
    data.repartition(1).saveAsTextFile(gpsPath)
    val kmeansData = hBaseRDD.map(x => Vectors.dense( x._2, x._3)).persist(StorageLevel.MEMORY_AND_DISK)
    var minCost = 0.toDouble
    val models = new HashMap[Int,KMeansModel]()
    var minK = 0
    val splits = kmeansData.randomSplit(Array(0.9, 0.1))
    val trainData = splits(0)
    val testData = splits(1)
    for(k <- 2 to maxK) {
      /*val model = KMeans.train(kmeansData, k, maxIterations)
      */
      //use DenseKMeans
      val model = new KMeans().setInitializationMode(initMode)
      .setK(k)
      .setMaxIterations(params.numIterations)
//      .run(trainData)
      .run(kmeansData)
      val cost = model.computeCost(kmeansData)
      models += (k -> model)
      if (cost <= minCost || minCost == 0) {
        minCost = cost
        minK = k
      } 
      
      println(s"Within Set Sum of Squared Errors = $cost")
    }
    
    ////
    //交叉评估，返回结果
//    val model = KMeans.train(trainData, minK, params.numIterations)
    val model = models.getOrElse(minK, null)
    if (model != null) {
      hBaseRDD.map {
        x =>
        val point = Vectors.dense(x._2, x._3)
        val prediction = model.predict(point)
        val cityId = String.valueOf(x._4)
        var p = cityId
        var c = cityId
        if (x._4 != null && cityId.length() >=4) {
          p = cityId.substring(0,4) 
        }
        if (x._4 != null && cityId.length() >=2) {
        	c = cityId.substring(0,2) 
        }
        x._1 + " "+x._2+" " +x._3+" " +x._4+" " +p+" " +c+" " + prediction  
      }.repartition(1).saveAsTextFile(predictedGPSPath)
    }
    for(m <- models) {
      val k=m._1
      val model = m._2
      println(s"[$k]--Cluster centers:")
      for(c<-model.clusterCenters) {
        println("---"+c.toString())
      }
    }
    println(s"Min K = $minK, Min SSE = $minCost")
    sc.stop()
  }
}