package com.ibm.KafkaSparkStreaming

import java.util.Properties

/**
  * Created by Edward on 2017-4-8.
  */
class UserBehaviorMsgProducer(brokers: String, topic: String) extends Runnable {
  private val brokerList = brokers
  private val targetTopic = topic
  private val props = new Properties()
  props.put("metadata.broker.list", this.brokerList)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")
  // Please use org.apache.kafka.clients.producer.ProducerConfig instead
  private val config = new kafka.producer.ProducerConfig(this.props)
  // Please use org.apache.kafka.clients.producer.KafkaProducer instead
  private val producer = new kafka.producer.Producer[String, String](this.config)

  override def run(): Unit = ???
}
