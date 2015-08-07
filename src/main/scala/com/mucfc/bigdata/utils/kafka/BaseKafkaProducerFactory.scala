/**
 *
 */
package com.mucfc.bigdata.utils.kafka

import java.util.Properties

/**
 * @author Eapha Yim
 * @time 2015年7月23日
 */
class BaseKafkaProducerFactory(config: Properties = new Properties, defaultTopic: Option[String] = None) extends KafkaProducerFactory(config, defaultTopic)  {
  override def newInstance() = new KafkaProducer(config, defaultTopic)
}