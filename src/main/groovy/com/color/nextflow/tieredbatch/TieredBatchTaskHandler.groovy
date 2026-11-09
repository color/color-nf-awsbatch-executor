package com.color.nextflow.tieredbatch

import groovy.util.logging.Slf4j
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.charset.StandardCharsets
import nextflow.cloud.aws.batch.AwsBatchTaskHandler
import nextflow.exception.ProcessUnrecoverableException
import nextflow.processor.TaskRun
import nextflow.trace.TraceRecord
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.services.batch.model.JobDetail
import software.amazon.awssdk.services.batch.model.RetryStrategy
import software.amazon.awssdk.services.batch.model.SubmitJobRequest

@Slf4j
class TieredBatchTaskHandler extends AwsBatchTaskHandler {
    private final TaskRetryStateStore retryStates
    private final String taskKey
    private final TieredRetryPolicy.Tier tier
    private final String selectedQueue
    private final BatchSubmissionPlugin submissionPlugin
    private JobDetail observedJob
    BatchFailureClassifier.Failure reportedFailure
    TieredRetryPolicy.Tier getReportedTier() { tier }
    private boolean failureRecorded

    TieredBatchTaskHandler(TaskRun task, TieredBatchExecutor executor, TaskRetryStateStore retryStates, String taskKey, TieredRetryPolicy.Tier tier, String queue) {
        super(task, executor)
        this.retryStates = retryStates
        this.taskKey = taskKey
        this.tier = tier
        selectedQueue = queue
        submissionPlugin = executor.submissionPlugin
    }

    @Override
    protected SubmitJobRequest newSubmitRequest(TaskRun task) {
        def request = super.newSubmitRequest(task)
        if (request.arrayProperties() || request.nodeOverrides() || request.ecsPropertiesOverride() || request.eksPropertiesOverride())
            throw new ProcessUnrecoverableException('tiered-awsbatch supports single-container EC2 jobs only')
        def tags = new LinkedHashMap<>(request.tags())
        tags.put('job-queue', selectedQueue)
        def override = request.overrideConfiguration().orElse(AwsRequestOverrideConfiguration.builder().build()).toBuilder()
            .addPlugin(submissionPlugin).build()
        return request.toBuilder().jobQueue(selectedQueue).tags(tags)
            .retryStrategy(RetryStrategy.builder().attempts(1).build())
            .overrideConfiguration(override).build()
    }

    @Override
    void submit() {
        try {
            super.submit()
            if (!getJobId())
                throw new ProcessUnrecoverableException('AWS Batch returned no job ID; submission outcome is unknown')
            retryStates.submitted(taskKey, task.config.attempt as int, getJobId())
            log.info "[tiered-awsbatch] task=${taskKey}; job=${getJobId()}; tier=${tier}; queue=${selectedQueue}"
        }
        catch (RuntimeException error) {
            throw new ProcessUnrecoverableException('AWS Batch submission failed or has an unknown outcome; inspect accepted jobs before resuming. Automatic resubmission is disabled.', error)
        }
    }

    @Override
    protected JobDetail describeJob(String jobId) {
        observedJob = super.describeJob(jobId)
        return observedJob
    }

    @Override
    boolean checkIfCompleted() {
        boolean completed = super.checkIfCompleted()
        if (!completed || failureRecorded || (!task.error && task.exitStatus == 0))
            return completed
        failureRecorded = true
        def classification = task.error instanceof ProcessUnrecoverableException ?
            BatchFailureClassifier.Failure.UNRECOVERABLE :
            BatchFailureClassifier.classify(observedJob, tier == TieredRetryPolicy.Tier.SPOT)
        if (classification == BatchFailureClassifier.Failure.ORDINARY)
            classification = BatchFailureClassifier.classify(observedJob, tier == TieredRetryPolicy.Tier.SPOT, failureDiagnostics())
        reportedFailure = classification
        def state = retryStates.failed(taskKey, getJobId(), tier, classification)
        log.info "[tiered-awsbatch] task=${taskKey}; job=${getJobId()}; classification=${classification}; interruptions=${state.interruptions}; infrastructureFailures=${state.infrastructureFailures}; ordinaryFailures=${state.ordinaryFailures}; reason=${observedJob?.statusReason()}"
        if (state.terminal) {
            task.config.put('errorStrategy', 'terminate')
            task.error = new ProcessUnrecoverableException("Tiered retry allowance exhausted: ${task.error?.message ?: observedJob?.statusReason()}", task.error)
        }
        return true
    }

    protected String failureDiagnostics() {
        if (task.stderr instanceof CharSequence)
            return task.stderr.toString().takeRight(65536)
        // Upstream uses CloudWatch output when available; otherwise inspect staged stderr.
        def diagnostics = []
        for (Path path : [task.stderr instanceof Path ? task.stderr : null, getLogFile()]) {
            if (!path)
                continue
            try {
                Files.newByteChannel(path).withCloseable { channel ->
                    channel.position(Math.max(0L, channel.size() - 65536L))
                    def buffer = ByteBuffer.allocate(65536)
                    while (buffer.hasRemaining() && channel.read(buffer) > 0) { }
                    buffer.flip()
                    diagnostics.add(StandardCharsets.UTF_8.decode(buffer).toString())
                }
            }
            catch (IOException | RuntimeException ignored) {
                log.debug "Unable to read failure diagnostics for ${taskKey}; retaining ordinary classification"
            }
        }
        return diagnostics.join('\n')
    }

    @Override
    TraceRecord getTraceRecord() {
        def trace = super.getTraceRecord()
        trace.put('queue', selectedQueue)
        return trace
    }
}
