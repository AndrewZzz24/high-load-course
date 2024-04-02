package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import ru.quipy.common.utils.RateLimiter
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
        private val properties4: ExternalServiceProperties,
        private val properties3: ExternalServiceProperties,
        private val properties2: ExternalServiceProperties,

        ) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val rateLimiter4 = RateLimiter(properties4.rateLimitPerSec)
    private val rateLimiter3 = RateLimiter(properties3.rateLimitPerSec)
    private val rateLimiter2 = RateLimiter(properties2.rateLimitPerSec)
    private val processTime4 = arrayListOf<Long>()
    private val processTime3 = arrayListOf<Long>()
    private val processTime2 = arrayListOf<Long>()

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val queue4 = LinkedBlockingQueue<PaymentInfo>(properties4.parallelRequests)
    private val queue3 = LinkedBlockingQueue<PaymentInfo>(properties3.parallelRequests)
    private val queue2 = LinkedBlockingQueue<PaymentInfo>(properties2.parallelRequests)

    private val accountExecutor4 = Executors.newFixedThreadPool(properties4.parallelRequests)
    private val accountExecutor3 = Executors.newFixedThreadPool(properties3.parallelRequests)
    private val accountExecutor2 = Executors.newFixedThreadPool(properties2.parallelRequests)

    private val client4 = OkHttpClient.Builder()
            .dispatcher(Dispatcher(accountExecutor4).apply { maxRequests = properties4.parallelRequests })
            .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
            .connectionPool(ConnectionPool(properties4.parallelRequests, paymentOperationTimeout.seconds, TimeUnit.SECONDS))
            .build()

    private val client3 = OkHttpClient.Builder()
            .dispatcher(Dispatcher(accountExecutor3).apply { maxRequests = properties3.parallelRequests })
            .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
            .connectionPool(ConnectionPool(properties3.parallelRequests, paymentOperationTimeout.seconds, TimeUnit.SECONDS))
            .build()

    private val client2 = OkHttpClient.Builder()
            .dispatcher(Dispatcher(accountExecutor2).apply { maxRequests = properties2.parallelRequests })
            .protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
            .connectionPool(ConnectionPool(properties2.parallelRequests, paymentOperationTimeout.seconds, TimeUnit.SECONDS))
            .build()

    private fun chooseAccount(paymentStartedAt: Long): Pair<ExternalServiceProperties, LinkedBlockingQueue<PaymentInfo>> {
        if (Duration.ofSeconds((now() - paymentStartedAt) / 1000) >= paymentOperationTimeout) {
            throw TimeoutException("Payment operation timed out.")
        }

        if (!rateLimiter4.tick()
                || queue4.remainingCapacity() <= 0) {
            if (rateLimiter3.tick()
                    && queue3.remainingCapacity() > 0
                    && Duration.ofSeconds((now() - paymentStartedAt) / 1000) < paymentOperationTimeout)
                return Pair(properties3, queue3)
            else {
                if (rateLimiter2.tick()
                        && queue2.remainingCapacity() > 0
                        && Duration.ofSeconds((now() - paymentStartedAt) / 1000) < paymentOperationTimeout)
                    return Pair(properties2, queue2)
                else
                    throw TimeoutException("Payment operation timed out.")
            }
        } else
            return Pair(properties4, queue4)
    }


    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        var (properties, queue) = Pair(properties4, queue4)
        try {
            val (newProperties, newQueue) = chooseAccount(paymentStartedAt)
            properties = newProperties
            queue = newQueue
        } catch (ex: TimeoutException) {
            return
        }

        logger.warn("[${properties.accountName}] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[${properties.accountName}] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val time = when (properties) {
            properties4 -> processTime4
            properties3 -> processTime3
            else -> processTime2
        }

        if (time.average() * queue.size + ((now() - paymentStartedAt) / 1000) >= paymentOperationTimeout.seconds) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
            return
        }

        queue.add(PaymentInfo(paymentId, transactionId, paymentStartedAt))

        val accountExecutor = when (properties) {
            properties4 -> accountExecutor4
            properties3 -> accountExecutor3
            else -> accountExecutor2
        }

        accountExecutor.submit {
            processPaymentRequest(queue, properties)
        }

        val speed = minOf(properties.parallelRequests.div(time.average()), properties.rateLimitPerSec.toDouble())
        logger.error("[${properties.accountName}] Theoretical speed for $paymentId , txId $transactionId : $speed")
    }

    private fun processPaymentRequest(queue: LinkedBlockingQueue<PaymentInfo>, properties: ExternalServiceProperties) {
        val paymentInfo = queue.poll()

        if (Duration.ofSeconds((now() - paymentInfo.paymentStartedAt) / 1000) >= paymentOperationTimeout) {
            paymentESService.update(paymentInfo.paymentId) {
                it.logProcessing(false, now(), paymentInfo.transactionId, reason = "Request timeout.")
            }
            return
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${properties.serviceName}&accountName=${properties.accountName}&transactionId=${paymentInfo.transactionId}")
            post(emptyBody)
        }.build()

        val client = when (properties) {
            properties4 -> client4
            properties3 -> client3
            else -> client2
        }

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                //                context.release()
                when (e) {
                    is SocketTimeoutException -> {
                        paymentESService.update(paymentInfo.paymentId) {
                            it.logProcessing(false, now(), paymentInfo.transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error("[${properties.accountName}] Payment failed for txId: ${paymentInfo.transactionId}," +
                                " payment: ${paymentInfo.paymentId}", e)

                        paymentESService.update(paymentInfo.paymentId) {
                            it.logProcessing(false, now(), paymentInfo.transactionId, reason = e.message)
                        }
                    }
                }
                val time = when (properties) {
                    properties4 -> processTime4
                    properties3 -> processTime3
                    else -> processTime2
                }

                time.add((now() - paymentInfo.paymentStartedAt) / 1000)
            }

            override fun onResponse(call: Call, response: Response) {
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[${properties.accountName}] [ERROR] Payment processed for txId: ${paymentInfo.transactionId}, payment: ${paymentInfo.paymentId}, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[${properties.accountName}] Payment processed for txId: ${paymentInfo.transactionId}," +
                        " payment: ${paymentInfo.paymentId}, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentInfo.paymentId) {
                    it.logProcessing(body.result, now(), paymentInfo.transactionId, reason = body.message)
                }
                val time = when (properties) {
                    properties4 -> processTime4
                    properties3 -> processTime3
                    else -> processTime2
                }

                time.add((now() - paymentInfo.paymentStartedAt) / 1000)
            }
        })
    }

//        try {
//            client.newCall(request).execute().use { response ->
//                val body = try {
//                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
//                } catch (e: Exception) {
//                    logger.error("[${properties.accountName}] [ERROR] Payment processed for txId: ${paymentInfo.transactionId}, payment: ${paymentInfo.paymentId}, result code: ${response.code}, reason: ${response.body?.string()}")
//                    ExternalSysResponse(false, e.message)
//                }
//
//                logger.warn("[${properties.accountName}] Payment processed for txId: ${paymentInfo.transactionId}," +
//                        " payment: ${paymentInfo.paymentId}, succeeded: ${body.result}, message: ${body.message}")
//
//                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
//                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
//                paymentESService.update(paymentInfo.paymentId) {
//                    it.logProcessing(body.result, now(), paymentInfo.transactionId, reason = body.message)
//                }
//            }
//        } catch (e: Exception) {
//            when (e) {
//                is SocketTimeoutException -> {
//                    paymentESService.update(paymentInfo.paymentId) {
//                        it.logProcessing(false, now(), paymentInfo.transactionId, reason = "Request timeout.")
//                    }
//                }
//
//                else -> {
//                    logger.error("[${properties.accountName}] Payment failed for txId: ${paymentInfo.transactionId}," +
//                            " payment: ${paymentInfo.paymentId}", e)
//
//                    paymentESService.update(paymentInfo.paymentId) {
//                        it.logProcessing(false, now(), paymentInfo.transactionId, reason = e.message)
//                    }
//                }
//            }
//        } finally {
//            val time = when (properties) {
//                properties4 -> processTime4
//                properties3 -> processTime3
//                else -> processTime2
//            }
//
//            time.add((now() - paymentInfo.paymentStartedAt) / 1000)
//        }

}

public fun now() = System.currentTimeMillis()

class PaymentInfo(
        val paymentId: UUID,
        val transactionId: UUID,
        val paymentStartedAt: Long
)
