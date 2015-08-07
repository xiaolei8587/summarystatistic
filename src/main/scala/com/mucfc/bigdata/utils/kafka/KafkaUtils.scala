/**
 *
 */
package com.mucfc.bigdata.utils.kafka

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.commons.pool2.impl.GenericObjectPool
import java.util.Properties
/**
 * @author Eapha Yim
 * @time 2015年7月23日
 */
object KafkaUtils {

  /**
   * 
   * config should contain :
   * producer.type = async
   * compression.codec =snappy
   * metadata.broker.list=
   * 
   * @param config kafka producer config
   * @param topic 
   */
  def createKafkaProducerPool(config: Properties, topic: String): GenericObjectPool[KafkaProducer] = {
    val producerFactory = new BaseKafkaProducerFactory(config, defaultTopic = Option(topic))
    val pooledProducerFactory = new PooledKafkaProducerFactory(producerFactory)
    
    val poolConfig = {
      val c = new GenericObjectPoolConfig
      val maxNumProducers = config.getProperty("producer.total.max", "10").toInt
      c.setMaxTotal(maxNumProducers)
      c.setMaxIdle(maxNumProducers)
      c
    }
    new GenericObjectPool[KafkaProducer](pooledProducerFactory, poolConfig)
  }
  
  /**
   * close kafka producer pool
   */
  def closeKafkaProducerPool(pool:GenericObjectPool[KafkaProducer]) {
    pool.close();
  }
}