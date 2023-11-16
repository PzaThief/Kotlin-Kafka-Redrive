package com.example.kotlinkafkaredrive

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.*


@Component
class Redriver {
    private fun consumerProperties(bootstrapServers: List<String>):Properties {
        val configs = Properties()
        configs[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        configs[ConsumerConfig.GROUP_ID_CONFIG] = "redrive-group"
        configs[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        configs[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        configs[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = OffsetResetStrategy.EARLIEST.toString()
        configs[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = IsolationLevel.READ_COMMITTED.toString().lowercase()
        return configs
    }

    private fun producerProperties(bootstrapServers: List<String>):Properties {
        val configs = Properties()
        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        configs[ProducerConfig.TRANSACTIONAL_ID_CONFIG] = UUID.randomUUID().toString()
        return configs
    }

    fun redrive(bootstrapServers:List<String>, sourceTopic: String, targetTopic: String){
        val consumer = KafkaConsumer<String, String>(consumerProperties(bootstrapServers))
        val topicPartitions = consumer.partitionsFor(sourceTopic).map { TopicPartition(it.topic(), it.partition()) }
        consumer.assign(topicPartitions)
        consumer.seekToBeginning(topicPartitions)
        val targetOffsets = consumer.endOffsets(topicPartitions).entries.associate {
            it.key.partition() to it.value-1
        }
        val offsetRemain = targetOffsets.mapValues { true }.toMutableMap()
        val recordToRedrive: MutableList<ConsumerRecord<String, String>> = mutableListOf()
        while (offsetRemain.any { it.value }) {
            val records = consumer.poll(Duration.ofSeconds(1))
            records.filter {
                targetOffsets.containsKey(it.partition()) && it.offset() <= targetOffsets[it.partition()]!!
            }.onEach {
                if (it.offset() == targetOffsets[it.partition()]) offsetRemain[it.partition()] = false
            }.let {
                recordToRedrive.addAll(it)
            }
        }
        LOGGER.info("got ${recordToRedrive.size} records to redrive")

        val producer = KafkaProducer<String, String>(producerProperties(bootstrapServers))
        producer.initTransactions()
        producer.beginTransaction()
        recordToRedrive.forEach {
            producer.send(ProducerRecord(targetTopic, it.key(), it.value()))
        }
        producer.commitTransaction()
        producer.close()
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(KafkaProducer::class.java)
    }

}