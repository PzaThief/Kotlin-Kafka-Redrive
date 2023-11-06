package com.example.kotlinkafkaredrive

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.RetryableTopic
import org.springframework.kafka.retrytopic.DltStrategy
import org.springframework.stereotype.Component


@Component
class Consumer(
    @Autowired val objectMapper: ObjectMapper,
) {
    @RetryableTopic(dltStrategy = DltStrategy.FAIL_ON_ERROR)
    @KafkaListener(topics = ["\${test.topic}"])
    fun receive(message: ConsumerRecord<String, String>) {
        LOGGER.info("received payload='{}'", message.toString())
        try {
            val event = objectMapper.readValue<ProductPriceChangedEvent>(message.value())
            // do something
        } catch (e: Exception) {
            LOGGER.info("error occurred while receive message in ${message.topic()} topic $e")
            throw e
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(KafkaConsumer::class.java)
    }

}

