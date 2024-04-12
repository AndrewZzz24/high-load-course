package ru.quipy.payments.config

import org.springframework.stereotype.Service
import java.time.Duration

@Service
class AccountService {
    val accounts = ExternalServicesConfig.properties.map { Account(it) }
    fun getTheCheapestAvailableAccount(paymentStartedAt: Long): Account? {
        return accounts.stream()
                .filter { account: Account ->
                    account.rateLimiter.tick() &&
                    Duration.ofSeconds((System.currentTimeMillis() - paymentStartedAt) / 1000) < account.paymentOperationTimeout
                }
                .min(Comparator.comparing { a -> a.properties.callCost })
                .orElse(null)
    }
}
