package ru.quipy.payments.config

import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Protocol
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.RateLimiter
import ru.quipy.payments.logic.ExternalServiceProperties
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class Account(
        val properties: ExternalServiceProperties
) {
    val processTime = arrayListOf<Long>()
    val rateLimiter = RateLimiter(properties.rateLimitPerSec)
    val paymentOperationTimeout: Duration = Duration.ofSeconds(80)

    private val httpClientExecutor: ExecutorService = Executors.newFixedThreadPool(properties.parallelRequests,
            NamedThreadFactory("${properties.accountName}-httpClientExecutor")
    )

    val accountExecutor: ExecutorService = Executors.newFixedThreadPool(100, NamedThreadFactory("${properties.accountName}-accountExecutor"))

    val responsePool: ExecutorService = Executors.newFixedThreadPool(100, NamedThreadFactory("${properties.accountName}-paymentResponse"))

    val httpClient = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor)
                .apply {
                    maxRequests = 400
                    maxRequestsPerHost = 400
                })
        connectionPool(ConnectionPool(100, 5, TimeUnit.MINUTES))
        protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        build()
    }
}
