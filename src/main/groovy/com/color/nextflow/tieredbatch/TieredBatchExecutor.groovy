package com.color.nextflow.tieredbatch

import com.google.common.util.concurrent.RateLimiter
import groovy.util.logging.Slf4j
import java.util.concurrent.TimeUnit
import nextflow.cloud.aws.batch.AwsBatchExecutor
import nextflow.exception.ProcessUnrecoverableException
import nextflow.processor.TaskArrayRun
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.util.ServiceName

@Slf4j
@ServiceName('tiered-awsbatch')
class TieredBatchExecutor extends AwsBatchExecutor {
    private TieredBatchConfig tieredConfig
    protected TaskRetryStateStore retryStates
    // Shared by every task and SDK retry; upstream's adaptive throttle can exceed 50/s.
    final BatchSubmissionPlugin submissionPlugin = new BatchSubmissionPlugin(RateLimiter.create(20d, 1, TimeUnit.SECONDS))

    @Override
    protected void register() {
        tieredConfig = new TieredBatchConfig(session.config.navigate('tieredAwsBatch') as Map ?: [:])
        if (!tieredConfig.enabled)
            throw new IllegalArgumentException('Set tieredAwsBatch.enabled = true to use tiered-awsbatch')
        if (session.config.navigate('fusion.enabled') == true || session.config.navigate('aws.batch.platformType') == 'fargate')
            throw new IllegalArgumentException('tiered-awsbatch supports non-Fusion, single-container EC2 jobs only')
        super.register()
        BatchQueueValidator.validate(getClient(), tieredConfig.queueMappings, awsOptions.region)
        retryStates = TaskRetryStateStore.forSession(session, tieredConfig)
        log.info 'Tiered AWS Batch retry budgets are scoped to this launch; resumed launches start fresh budgets'
    }

    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        if (task instanceof TaskArrayRun || task.config.get('array'))
            throw new ProcessUnrecoverableException('tiered-awsbatch does not support job arrays')
        def strategy = task.config.getRawValue('errorStrategy')
        if (strategy?.toString() != 'retry')
            throw new ProcessUnrecoverableException('tiered-awsbatch requires static errorStrategy = retry; remove process-specific overrides')
        if (task.config.maxRetries < tieredConfig.interruptionThreshold + tieredConfig.infrastructureFailureThreshold + tieredConfig.ordinaryRetryAllowance || task.config.maxErrors != -1)
            throw new ProcessUnrecoverableException('tiered-awsbatch requires maxErrors = -1 and maxRetries >= interruptionThreshold + infrastructureFailureThreshold + ordinaryRetryAllowance')
        if (task.getContainer()?.startsWith('job-definition://'))
            throw new ProcessUnrecoverableException('tiered-awsbatch requires a Docker image; pre-existing job definitions are unsupported')
        String key = "${task.processor.name}:${task.index}"
        def state = retryStates.prepare(key, task.config.queue.toString(), task.config.attempt as int)
        def tier = retryStates.policy.tier(state)
        if (tier == TieredRetryPolicy.Tier.ON_DEMAND)
            task.config.put('errorStrategy', 'terminate')
        return newTaskHandler(task, key, tier, retryStates.policy.queue(state))
    }

    protected TieredBatchTaskHandler newTaskHandler(TaskRun task, String key, TieredRetryPolicy.Tier tier, String queue) {
        return new TieredBatchTaskHandler(task, this, retryStates, key, tier, queue)
    }
}
