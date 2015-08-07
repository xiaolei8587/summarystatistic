package com.mucfc.bigdata.utils.kafka

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object ConvertKafka {
  implicit def getKafkaRDD[V: ClassTag](rdd: RDD[V]): KafkaRDD[V] = new KafkaRDD(rdd)
}