/**
 *
 */
package com.mucfc.bigdata.utils.kafka

import org.apache.spark.Logging
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.commons.pool2.impl.GenericObjectPool
import java.util.Properties


/**
 * @author Eapha Yim
 * @time 2015年7月23日
 */
class KafkaRDD[V: ClassTag](rdd: RDD[V]) extends Logging with Serializable {
  /**
   * send data to kafka
   * @param topic kafka message topic
   * @param producerPool GenericObjectPool[KafkaProducer]
   */
 def send2kafka(topic: String, producerPool:Broadcast[GenericObjectPool[KafkaProducer]], f: (V) => String) = {
    rdd.foreachPartition(iter =>
      
      if (iter.isEmpty) {
        log.warn("The iter is empty!no data send")
      } else {
        log.info("start to send data")
        val p = producerPool.value.borrowObject()
        iter.foreach { x => p.send(f(x).getBytes, topic) }
        producerPool.value.returnObject(p)
        log.info("end to send data")
      })
  }
  def sendData2kafka(topic: String, kafkCfg:Broadcast[Properties], f: (V) => String) = {
	  rdd.foreachPartition(iter =>
	  if (iter.isEmpty) {
		  log.warn("The iter is empty!no data send")
	  } else {
		  log.info("start to send data")
      val cfg = kafkCfg.value
		  val p = new KafkaProducer(cfg,Some(topic))
		  iter.foreach { x => p.send(f(x).getBytes, topic) }
      p.shutdown()
		  log.info("end to send data")
	  })
  }
}