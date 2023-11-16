package com.example.kotlinkafkaredrive

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.TestPropertySource
import org.testcontainers.containers.KafkaContainer
import java.math.BigDecimal
import java.time.Duration
import java.util.concurrent.TimeUnit


@SpringBootTest
@TestPropertySource(
    properties = [
        "spring.kafka.consumer.auto-offset-reset=earliest"
    ]
)
@Import(TestContainerConfiguration::class)
class KotlinKafkaRedriveApplicationTests(
    @Autowired
    val objectMapper: ObjectMapper,
    @Autowired
    val kafkaContainer: KafkaContainer,
    @Autowired
    val producer: Producer,
    @Autowired
    val kafkaTemplate: KafkaTemplate<String, String>,
    @Autowired
    val kafkaProperties: KafkaProperties,
    @Autowired
    val redriver: Redriver,
    @Value("\${test.topic}")
    val topicName: String
) {
    val dltTopicName = "$topicName-dlt"
    data class UnverifiedEvent(
        val productCode: String,
        val changeAmount: BigDecimal
    )

    @BeforeEach
    fun checkKafkaContainer() {
        assert(kafkaContainer.isRunning)
        val props = kafkaProperties.buildConsumerProperties()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.bootstrapServers
        props.remove(ConsumerConfig.GROUP_ID_CONFIG)
        kafkaTemplate.setConsumerFactory(DefaultKafkaConsumerFactory(props))
    }

    @Test
    fun producerAndConsumerWork_Success() {
        val event = ProductPriceChangedEvent(
            ProductCode.P100,
            BigDecimal("14.50")
        )
        val future = producer.send(topicName, objectMapper.writeValueAsString(event))
        future.get(5, TimeUnit.SECONDS)
        assert(future.isDone)

        val consumerRecord = kafkaTemplate.receive(topicName, 0, 0, Duration.ofSeconds(5))
        assertThat(consumerRecord?.value()).isNotNull
        val consumedEvent = objectMapper.readValue<ProductPriceChangedEvent>(consumerRecord!!.value())
        assertThat(consumedEvent).isEqualTo(event)
    }

    @Test
    fun transferToDLT_Success() {
        val event = UnverifiedEvent(
            "P999",
            BigDecimal("14.50")
        )
        val future = producer.send(topicName, objectMapper.writeValueAsString(event))
        future.get(5, TimeUnit.SECONDS)
        assert(future.isDone)

        val consumerRecord = kafkaTemplate.receive(topicName, 0, 0, Duration.ofSeconds(5))
        assertThat(consumerRecord?.value()).isNotNull
        assertThrows<Exception> {
            objectMapper.readValue<ProductPriceChangedEvent>(consumerRecord!!.value())
        }

        val dltConsumerRecord = kafkaTemplate.receive(dltTopicName, 0, 0, Duration.ofSeconds(5))
        assertThat(dltConsumerRecord?.value()).isNotNull
    }

    @Test
    fun simpleRedrive_Success() {
        val event = UnverifiedEvent(
            "P999",
            BigDecimal("14.50")
        )
        val future = producer.send(topicName, objectMapper.writeValueAsString(event))
        future.get(5, TimeUnit.SECONDS)
        assert(future.isDone)

        kafkaTemplate.receive(topicName, 0, 0, Duration.ofSeconds(5))
            .also {
                assertThat(it?.value()).isNotNull
                assertThrows<Exception> {
                    objectMapper.readValue<ProductPriceChangedEvent>(it!!.value())
                }
            }

        kafkaTemplate.receive(topicName, 0, 1, Duration.ofSeconds(5))
            .also {
                assertThat(it?.value()).isNull()
            }

        kafkaTemplate.receive(dltTopicName, 0, 0, Duration.ofSeconds(5))
            .also {
                assertThat(it?.value()).isNotNull
            }

        redriver.redrive(listOf(kafkaContainer.bootstrapServers), dltTopicName, topicName)

        kafkaTemplate.receive(topicName, 0, 1, Duration.ofSeconds(5))
            .also {
                assertThat(it?.value()).isNotNull
                assertThat(objectMapper.readValue<UnverifiedEvent>(it!!.value())).isEqualTo(event)
            }
    }
}
