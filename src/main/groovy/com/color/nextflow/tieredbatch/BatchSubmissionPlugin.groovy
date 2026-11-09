package com.color.nextflow.tieredbatch

import com.google.common.util.concurrent.RateLimiter
import java.util.function.Predicate
import software.amazon.awssdk.core.SdkPlugin
import software.amazon.awssdk.core.SdkServiceClientConfiguration
import software.amazon.awssdk.core.interceptor.Context
import software.amazon.awssdk.core.interceptor.ExecutionAttributes
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.batch.model.BatchException

/** SubmitJob has no idempotency token: only an explicit throttling rejection is safe to retry. */
class BatchSubmissionPlugin implements SdkPlugin, ExecutionInterceptor {
    private final RateLimiter limiter

    BatchSubmissionPlugin(RateLimiter limiter) {
        this.limiter = limiter
    }

    @Override
    void configureClient(SdkServiceClientConfiguration.Builder builder) {
        Predicate<Throwable> throttlingRejection = { Throwable error ->
            error instanceof BatchException && error.statusCode() == 429
        }
        builder.overrideConfiguration(builder.overrideConfiguration().toBuilder()
            .addExecutionInterceptor(this)
            .retryStrategy(StandardRetryStrategy.builder().maxAttempts(5)
                .useClientDefaults(false)
                .retryOnException(throttlingRejection)
                .treatAsThrottling(throttlingRejection)
                .build()).build())
    }

    @Override
    void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes attributes) {
        limiter.acquire()
    }
}
