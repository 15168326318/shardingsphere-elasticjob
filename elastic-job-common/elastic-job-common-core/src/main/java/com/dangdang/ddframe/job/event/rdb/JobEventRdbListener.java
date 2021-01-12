/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.event.rdb;

import com.dangdang.ddframe.job.context.ExecutionType;
import com.dangdang.ddframe.job.event.JobEventListener;
import com.dangdang.ddframe.job.event.type.JobExecutionEvent;
import com.dangdang.ddframe.job.event.type.JobStatusTraceEvent;

import javax.sql.DataSource;
import java.sql.SQLException;
import org.apache.shardingsphere.elasticjob.tracing.rdb.storage.RDBJobEventStorage;

/**
 * 运行痕迹事件数据库监听器.
 *
 * @author caohao
 */
public final class JobEventRdbListener extends JobEventRdbIdentity implements JobEventListener {
    
    private final RDBJobEventStorage repository;
    
    public JobEventRdbListener(final DataSource dataSource) throws SQLException {
       repository = new RDBJobEventStorage(dataSource);
    }
    
    @Override
    public void listen(final JobExecutionEvent executionEvent)
    {
        org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent jobExecutionEvent = new org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent(
            executionEvent.getHostname(),
            executionEvent.getIp(),
            executionEvent.getTaskId(),
            executionEvent.getJobName(),
            this.getExecutionSource(executionEvent.getSource()),
            executionEvent.getShardingItem()
        );
        repository.addJobExecutionEvent(jobExecutionEvent);
    }
    
    @Override
    public void listen(final JobStatusTraceEvent jobStatusTraceEventOld) {
        org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent jobStatusTraceEvent = new org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent(
            jobStatusTraceEventOld.getJobName(),
            jobStatusTraceEventOld.getTaskId(),
            jobStatusTraceEventOld.getSlaveId(),
            this.getSource(jobStatusTraceEventOld.getSource()),
            jobStatusTraceEventOld.getExecutionType().name(),
            jobStatusTraceEventOld.getShardingItems(),
            this.getState(jobStatusTraceEventOld.getState()),
            jobStatusTraceEventOld.getMessage()
        );

        jobStatusTraceEvent.setOriginalTaskId(jobStatusTraceEventOld.getTaskId());
        repository.addJobStatusTraceEvent(jobStatusTraceEvent);
    }

    private org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent.ExecutionSource getExecutionSource(JobExecutionEvent.ExecutionSource executionSource) {
        switch (executionSource) {
            case MISFIRE:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent.ExecutionSource.MISFIRE;
            case FAILOVER:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent.ExecutionSource.FAILOVER;
            case NORMAL_TRIGGER:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent.ExecutionSource.NORMAL_TRIGGER;
            default:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent.ExecutionSource.MISFIRE;
        }
    }


    private org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.Source getSource(JobStatusTraceEvent.Source executionSource) {
        switch (executionSource) {
            case CLOUD_EXECUTOR:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.Source.CLOUD_EXECUTOR;
            case CLOUD_SCHEDULER:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.Source.CLOUD_SCHEDULER;
            case LITE_EXECUTOR:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.Source.LITE_EXECUTOR;
            default:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.Source.LITE_EXECUTOR;
        }
    }

    private org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State getState(JobStatusTraceEvent.State state) {
        switch (state) {
            case TASK_DROPPED:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State.TASK_DROPPED;
            case TASK_ERROR:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State.TASK_ERROR;
            case TASK_FAILED:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State.TASK_FAILED;
            case TASK_FINISHED:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State.TASK_FINISHED;
            case TASK_GONE:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State.TASK_GONE;
            case TASK_GONE_BY_OPERATOR:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State.TASK_GONE_BY_OPERATOR;
            case TASK_KILLED:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State.TASK_KILLED;
            case TASK_LOST:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State.TASK_LOST;
            case TASK_RUNNING:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State.TASK_RUNNING;
            case TASK_STAGING:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State.TASK_STAGING;
            case TASK_UNREACHABLE:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State.TASK_UNREACHABLE;
            default:
                return org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent.State.TASK_FINISHED;
        }
    }
}
