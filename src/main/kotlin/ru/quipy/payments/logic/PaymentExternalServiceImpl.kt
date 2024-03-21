package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties1: ExternalServiceProperties,
    private val properties2: ExternalServiceProperties,

    ) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)
        val requestSenderThreadPool = Executors.newFixedThreadPool(500, NamedThreadFactory("request-executor"))
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName1 = properties1.serviceName
    private val accountName1 = properties1.accountName
    private val serviceName2 = properties2.serviceName
    private val accountName2 = properties2.accountName

    private val window1 = NonBlockingOngoingWindow(properties1.parallelRequests)
    private val window2 = NonBlockingOngoingWindow(properties2.parallelRequests)
    private val rateLimiter1 = RateLimiter(properties1.rateLimitPerSec)
    private val rateLimiter2 = RateLimiter(properties2.rateLimitPerSec)
    private val processTime1 = arrayListOf<Long>()
    private val processTime2 = arrayListOf<Long>()

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newSingleThreadExecutor()

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        build()
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        var accountName = accountName2
        var serviceName = serviceName2

        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        var window = window2.putIntoWindow()
        if (!rateLimiter2.tick()
            || window is NonBlockingOngoingWindow.WindowResponse.Fail
        ) {
            if (window is NonBlockingOngoingWindow.WindowResponse.Success)
                window2.releaseWindow()
            window = window1.putIntoWindow()
            if (rateLimiter1.tick() && window is NonBlockingOngoingWindow.WindowResponse.Success &&
                Duration.ofSeconds((now() - paymentStartedAt) / 1000) < paymentOperationTimeout
            ) {
                accountName = accountName1
                serviceName = serviceName1
                val speed =
                    minOf(properties1.parallelRequests.div(processTime1.average()).toInt(), properties1.rateLimitPerSec)
                logger.error("[$accountName] Theoretical speed for $paymentId , txId $transactionId : $speed")
            } else {
                if (window is NonBlockingOngoingWindow.WindowResponse.Success)
                    window1.releaseWindow()
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
                return
            }
        } else {
            val speed =
                minOf(properties2.parallelRequests.div(processTime2.average()).toInt(), properties2.rateLimitPerSec)
            logger.error("[$accountName] Theoretical speed for $paymentId , txId $transactionId : $speed")
        }

        requestSenderThreadPool.submit {
            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
                post(emptyBody)
            }.build()

            try {
                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                if (accountName == accountName2) {
                    window2.releaseWindow()
                    processTime2.add((now() - paymentStartedAt) / 1000)
                }
                if (accountName == accountName1) {
                    window1.releaseWindow()
                    processTime1.add((now() - paymentStartedAt) / 1000)
                }
            }
        }
    }
}

public fun now() = System.currentTimeMillis()
