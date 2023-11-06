package com.example.kotlinkafkaredrive

import java.math.BigDecimal

data class ProductPriceChangedEvent(
    val productCode: String,
    val changeAmount: BigDecimal
)
