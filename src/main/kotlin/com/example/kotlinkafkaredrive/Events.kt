package com.example.kotlinkafkaredrive

import java.math.BigDecimal

data class ProductPriceChangedEvent(
    val productCode: ProductCode,
    val changeAmount: BigDecimal
)

enum class ProductCode {
    P100,
    P200
}