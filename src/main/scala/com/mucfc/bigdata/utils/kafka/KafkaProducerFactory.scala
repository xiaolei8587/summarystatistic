/**
 *
 */
package com.mucfc.bigdata.utils.kafka

import java.util.Properties

/**
 * @author Eapha Yim
 * @time 2015年7月23日
 * @param config kafka config
 * @param topic kafka topic
 */
abstract class KafkaProducerFactory(config: Properties, topic: Option[String] = None) extends Serializable  {
  def newInstance(): KafkaProducer
}