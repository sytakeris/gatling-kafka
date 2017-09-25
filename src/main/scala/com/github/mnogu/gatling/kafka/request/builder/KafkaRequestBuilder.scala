package com.github.mnogu.gatling.kafka.request.builder

import com.github.mnogu.gatling.kafka.action.KafkaRequestActionBuilder
import io.gatling.core.session._
import org.apache.kafka.common.internals.Topic

case class KafkaAttributes[K,V]( requestName: Expression[String],
                                 key: Option[Expression[K]],
                                 payload: Expression[V],
                                 topic: String
                               )

case class KafkaRequestBuilder(requestName: Expression[String], topic: String = "") {
  def topic(name: String): KafkaRequestBuilder = copy(topic = name)

  def send[V](payload: Expression[V]): KafkaRequestActionBuilder[_,V] = send(payload, None)

  def send[K,V](key: Expression[K], payload: Expression[V]): KafkaRequestActionBuilder[K,V] = send(payload, Some(key))

  private def send[K,V](payload: Expression[V], key: Option[Expression[K]]) =
    new KafkaRequestActionBuilder(KafkaAttributes(requestName, key, payload, topic))

}
