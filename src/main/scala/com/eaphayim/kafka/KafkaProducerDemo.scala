/**
 *
 */
package com.eaphayim.kafka

import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Properties
import java.io.InputStreamReader
import java.io.File
import java.io.InputStream
import java.io.FileInputStream
import com.mucfc.bigdata.utils.kafka.KafkaUtils
import com.mucfc.bigdata.utils.kafka.KafkaProducer

/**
 * @author Eapha Yim
 * @time 2015年8月13日
 */
object KafkaProducerDemo extends Logging {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaProducerDemo")
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.parallelize(1 to 100)
    import com.mucfc.bigdata.utils.kafka.ConvertKafka._
    val kafkaConf = {
      val p = new Properties()
      val conf = "kafka.properties"
      p.load(new InputStreamReader(this.getClass.getClassLoader().getResourceAsStream(
      conf), "UTF-8"))
      p
    }
    
    
    val topic = sc.broadcast(kafkaConf.get("topic.demo").toString())
    kafkaConf.remove("topic.demo")
    val kafkCfg = sc.broadcast(kafkaConf)
//    val pool = KafkaUtils.createKafkaProducerPool(kafkaConf, topic)
//    val producerPool = sc.broadcast(pool)
    rdd.sendData2kafka(topic.value, kafkCfg, x => {
      x.toString()
    })
    rdd.foreachPartition { iter =>
    if (iter.isEmpty) {
      log.warn("The iter is empty!no data send")
    } else {
      log.info("start to send data")
      val cfg = kafkCfg.value
      val p = new KafkaProducer(cfg,Some(topic.value))
      iter.foreach { x => p.send(x.toString().getBytes, topic.value) }
      p.shutdown()
      log.info("end to send data")
    } }
    println("send kafka finished!")
//    KafkaUtils.closeKafkaProducerPool(pool)
    sc.stop()
  }
}