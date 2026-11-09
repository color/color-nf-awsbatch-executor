package com.color.nextflow.tieredbatch

import com.google.common.util.concurrent.RateLimiter
import com.sun.net.httpserver.HttpServer
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.batch.BatchClient
import software.amazon.awssdk.services.batch.model.BatchException
import software.amazon.awssdk.services.batch.model.SubmitJobRequest
import spock.lang.Specification
import spock.lang.Unroll

class BatchSubmissionPluginTest extends Specification {
    def calls = new AtomicInteger()
    def limiter = Mock(RateLimiter)
    HttpServer server
    BatchClient client

    def cleanup() {
        client?.close()
        server?.stop(0)
    }

    def 'explicit throttling is retried and every HTTP attempt takes a shared permit'() {
        given:
        startServer { exchange ->
            int attempt = calls.incrementAndGet()
            respond(exchange, attempt == 1 ? 429 : 200)
        }

        when:
        def response = client.submitJob(request())

        then:
        response.jobId() == 'accepted-job'
        calls.get() == 2
        2 * limiter.acquire() >> 0.0d
    }

    def 'persistent throttling stops after five attempts'() {
        given:
        startServer { exchange ->
            calls.incrementAndGet()
            respond(exchange, 429)
        }

        when:
        client.submitJob(request())

        then:
        thrown(BatchException)
        calls.get() == 5
        5 * limiter.acquire() >> 0.0d
    }

    @Unroll
    def 'HTTP #status is never retried even when client enables retries'() {
        given:
        startServer { exchange ->
            calls.incrementAndGet()
            respond(exchange, status)
        }

        when:
        client.submitJob(request())

        then:
        thrown(BatchException)
        calls.get() == 1
        1 * limiter.acquire() >> 0.0d

        where:
        status << [400, 500, 502, 503, 504]
    }

    def 'a lost response is not retried'() {
        given:
        def responseRelease = new CountDownLatch(1)
        startServer { exchange ->
            calls.incrementAndGet()
            responseRelease.await()
            exchange.close()
        }

        when:
        client.submitJob(request().toBuilder().overrideConfiguration(
            AwsRequestOverrideConfiguration.builder().addPlugin(new BatchSubmissionPlugin(limiter))
                .apiCallAttemptTimeout(Duration.ofMillis(500)).build()).build())

        then:
        thrown(SdkClientException)
        calls.get() == 1
        1 * limiter.acquire() >> 0.0d

        cleanup:
        responseRelease.countDown()
    }

    def 'independent submissions keep using the shared limiter after more than 100 successes'() {
        given:
        startServer { exchange ->
            calls.incrementAndGet()
            respond(exchange, 200)
        }

        when:
        def responses = (1..110).collect { client.submitJob(request()).jobId() }

        then:
        responses == ['accepted-job'] * 110
        calls.get() == 110
        110 * limiter.acquire() >> 0.0d
    }

    private SubmitJobRequest request() {
        return SubmitJobRequest.builder().jobName('fixture').jobQueue('spot').jobDefinition('fixture')
            .overrideConfiguration(AwsRequestOverrideConfiguration.builder()
                .addPlugin(new BatchSubmissionPlugin(limiter)).build()).build()
    }

    private void startServer(Closure handler) {
        server = HttpServer.create(new InetSocketAddress('127.0.0.1', 0), 0)
        server.createContext('/', handler as com.sun.net.httpserver.HttpHandler)
        server.start()
        client = BatchClient.builder().region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create('fixture', 'fixture')))
            .endpointOverride(new URI("http://127.0.0.1:${server.address.port}"))
            .overrideConfiguration(ClientOverrideConfiguration.builder()
                .retryStrategy(StandardRetryStrategy.builder().maxAttempts(10).build()).build()).build()
    }

    private static void respond(exchange, int status) {
        byte[] response = (status == 200 ? '{"jobId":"accepted-job","jobName":"fixture"}' :
            '{"message":"fixture error"}').bytes
        exchange.responseHeaders.add('Content-Type', 'application/json')
        if (status == 429)
            exchange.responseHeaders.add('x-amzn-ErrorType', 'TooManyRequestsException')
        exchange.sendResponseHeaders(status, response.length)
        exchange.responseBody.withCloseable { it.write(response) }
    }
}
