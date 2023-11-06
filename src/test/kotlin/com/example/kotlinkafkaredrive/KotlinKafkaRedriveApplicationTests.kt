package com.example.kotlinkafkaredrive

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.TestPropertySource
import org.testcontainers.containers.KafkaContainer
import java.math.BigDecimal
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
    val kafkaContainer: KafkaContainer,
    @Autowired
    val kafkaTemplate: KafkaTemplate<String, Any>
) {
    companion object {
        private const val topicName = "product-price-changes"
    }

    @BeforeEach
    fun checkKafkaContainer() {
        assert(kafkaContainer.isRunning)
    }

    @Test
    fun producerSend_Success() {
        val event = ProductPriceChangedEvent(
            "P100",
            BigDecimal("14.50")
        )
        val future = kafkaTemplate.send(topicName, event)
        future.get(5, TimeUnit.SECONDS)
        assert(future.isDone)
    }

}
