/**
 *
 */
package com.mucfc.bigdata.utils.kafka

import org.apache.commons.pool2.{ PooledObject, BasePooledObjectFactory }
import org.apache.commons.pool2.impl.DefaultPooledObject

/**
 * @author Eapha Yim
 * @time 2015年7月23日
 */
class PooledKafkaProducerFactory(val factory: KafkaProducerFactory) extends BasePooledObjectFactory[KafkaProducer] with Serializable {
  override def create(): KafkaProducer = factory.newInstance()

  override def wrap(obj: KafkaProducer): PooledObject[KafkaProducer] = new DefaultPooledObject(obj)

  override def destroyObject(p: PooledObject[KafkaProducer]): Unit = {
    p.getObject.shutdown()
    super.destroyObject(p)
  }
}