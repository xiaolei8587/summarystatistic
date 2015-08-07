/**
 *
 */
package com.mucfc.bigdata.utils.kafka

import org.apache.spark.Logging
import java.util.Properties
import kafka.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig

/**
 * @author Eapha Yim
 * @time 2015年7月23日
 */
class KafkaProducer(producerConfig: Properties = new Properties,
                    defaultTopic: Option[String] = None,
                    producer: Option[Producer[Array[Byte], Array[Byte]]] = None) extends Logging {

  type Key = Array[Byte]
  type Val = Array[Byte]
  require(!producerConfig.isEmpty(), "Producer config must not be empty")
  
  private val p = producer getOrElse {
    new Producer[Array[Byte], Array[Byte]](new ProducerConfig(producerConfig))
  } 
  private def toMessage(value: Val, key: Option[Key] = None, topic: Option[String] = None): KeyedMessage[Key, Val] = {
    val t = topic.getOrElse(defaultTopic.getOrElse(throw new IllegalArgumentException("Must provide topic or default topic")))
    require(!t.isEmpty, "Topic must not be empty")
    key match {
      case Some(k) => new KeyedMessage(t, k, value)
      case _ => new KeyedMessage(t, value)
    }
  }

  

  def send(key: Key, value: Val, topic: Option[String] = None) {
    p.send(toMessage(value, Option(key), topic))
  }

  def send(value: Val, topic: Option[String]) {
    send(null, value, topic)
  }

  def send(value: Val, topic: String) {
    send(null, value, Option(topic))
  }

  def send(value: Val) {
    send(null, value, None)
  }

  def shutdown(): Unit = p.close()

}