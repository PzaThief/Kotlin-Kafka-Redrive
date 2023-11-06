package com.example.kotlinkafkaredrive

import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Component
import java.util.concurrent.CompletableFuture


@Component
class Producer(
    @Autowired
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    fun send(topic: String, payload: Any): CompletableFuture<SendResult<String, Any>> {
        LOGGER.info("sending payload='{}' to topic='{}'", payload, topic)
        return kafkaTemplate.send(topic, payload)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(KafkaProducer::class.java)
    }
}

