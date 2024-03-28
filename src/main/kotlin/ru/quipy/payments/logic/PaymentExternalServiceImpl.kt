package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties1: ExternalServiceProperties,
    private val properties2: ExternalServiceProperties,

    ) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        val appExecutor = Executors.newFixedThreadPool(500, NamedThreadFactory("main-app-executor"))
//        val processPaymentRequestScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    }

    private var parallelRequestsCounter1 = AtomicInteger(0)
    private var parallelRequestsCounter2 = AtomicInteger(0)
    private val serviceName1 = properties1.serviceName
    private val accountName1 = properties1.accountName
    private val serviceName2 = properties2.serviceName
    private val accountName2 = properties2.accountName
    private val parallelRequests1 = properties1.parallelRequests
    private val parallelRequests2 = properties2.parallelRequests
    private val processTime1 = arrayListOf<Long>()
    private val processTime2 = arrayListOf<Long>()

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newSingleThreadExecutor()

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        build()
    }

    private fun decrementRequests(accountName: String) {
        if (accountName == accountName2)
            parallelRequestsCounter2.decrementAndGet()
        else
            parallelRequestsCounter1.decrementAndGet()
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        var accountName: String
        var serviceName: String
        while (true) {
            accountName = accountName2
            serviceName = serviceName2

            val curParReq1 = parallelRequestsCounter1.get()
            val curParReq2 = parallelRequestsCounter2.get()

            if (curParReq2 >= parallelRequests2 || Duration.ofSeconds((now() - paymentStartedAt) / 1000) > Duration.ofSeconds(
                    10
                )
            ) {
                accountName = accountName1
                serviceName = serviceName1
                if (parallelRequestsCounter1.compareAndSet(curParReq1, curParReq1 + 1))
                    break
            } else
                if (parallelRequestsCounter2.compareAndSet(curParReq2, curParReq2 + 1))
                    break
        }

        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        if (Duration.ofSeconds((now() - paymentStartedAt) / 1000) > paymentOperationTimeout
            || parallelRequestsCounter1.get() > parallelRequests1
        ) {
            decrementRequests(accountName)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
        } else {
            if (accountName.equals(accountName1)) {
                val speed = properties1.parallelRequests.div(processTime1.average()).toInt()
                logger.error("[$accountName] Theoretical speed for $paymentId , txId $transactionId : $speed")
            } else {
                val speed = properties2.parallelRequests.div(processTime2.average()).toInt()
                logger.error("[$accountName] Theoretical speed for $paymentId , txId $transactionId : $speed")
            }
            processPaymentRequest(serviceName, accountName, transactionId, paymentId, paymentStartedAt)
        }
    }


    private fun processPaymentRequest(
        serviceName: String,
        accountName: String,
        transactionId: UUID,
        paymentId: UUID,
        paymentStartedAt: Long
    ) {
//        CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher()).launch {
        appExecutor.submit {
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
                    processTime2.add((now() - paymentStartedAt) / 1000)
                } else {
                    processTime1.add((now() - paymentStartedAt) / 1000)
                }
                decrementRequests(accountName)
            }
        }
    }
}

public fun now() = System.currentTimeMillis()
