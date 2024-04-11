package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import javassist.NotFoundException
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.Account
import ru.quipy.payments.config.AccountService
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import java.util.concurrent.TimeoutException


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
        private val accountService: AccountService,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>


    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val account: Account
        try {
            account =
                    accountService.getTheCheapestAvailableAccount(paymentStartedAt)
                            ?: throw NotFoundException("There is no available account")
        } catch (e: Exception) {
            return
        }

        val accountName = account.properties.accountName
        val transactionId = UUID.randomUUID()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        account.accountExecutor.submit {
            try {
                processPaymentRequest(paymentId, transactionId, account, paymentStartedAt)
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
            }
        }
    }

    private fun processPaymentRequest(paymentId: UUID, transactionId: UUID, account: Account, paymentStartedAt: Long) {
        if (Duration.ofSeconds((now() - paymentStartedAt) / 1000) >= account.paymentOperationTimeout) {
            throw TimeoutException("Payment operation timed out.")
        }

        val accountName = account.properties.accountName
        val serviceName = account.properties.serviceName

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        account.httpClient.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                account.responsePool.submit {
                    when (e) {
                        is SocketTimeoutException -> {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }

                        else -> {
                            logger.error(
                                    "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e
                            )

                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                    val speed = minOf(account.processTime.average(), account.properties.rateLimitPerSec.toDouble())
                    logger.error("[$accountName] Theoretical speed for $paymentId , txId $transactionId : $speed")
                }
            }

            override fun onResponse(call: Call, response: Response) {
                account.responsePool.submit {
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
                account.processTime.add((now() - paymentStartedAt) / 1000)
                val speed = minOf(account.processTime.average(), account.properties.rateLimitPerSec.toDouble())
                logger.error("[$accountName] Theoretical speed for $paymentId , txId $transactionId : $speed")
            }
        })
    }
}
fun now() = System.currentTimeMillis()
