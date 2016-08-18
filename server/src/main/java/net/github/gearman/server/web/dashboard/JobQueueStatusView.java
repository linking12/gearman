package net.github.gearman.server.web.dashboard;

import net.github.gearman.engine.metrics.QueueMetrics;
import net.github.gearman.server.util.JobQueueMetrics;
import net.github.gearman.server.util.JobQueueMonitor;

public class JobQueueStatusView extends StatusView {

    private final String jobQueueName;

    public JobQueueStatusView(final JobQueueMonitor jobQueueMonitor, final QueueMetrics queueMetrics,
                              final String jobQueueName){
        super(jobQueueMonitor, queueMetrics);
        this.jobQueueName = jobQueueName;
    }

    public String getJobQueueName() {
        return jobQueueName;
    }

    public JobQueueMetrics getJobQueueSnapshots() {
        return this.getJobQueueSnapshots(this.jobQueueName);
    }

    public Long getNumberOfConnectedWorkers() {
        return queueMetrics.getActiveWorkers(jobQueueName);
    }

    public Long getActiveJobCount() {
        return queueMetrics.getActiveJobCount(jobQueueName);
    }

    public long getEnqueuedJobCount() {
        return queueMetrics.getEnqueuedJobCount(jobQueueName);
    }

    public long getCompletedJobCount() {
        return queueMetrics.getCompletedJobCount(jobQueueName);
    }

    public long getFailedJobCount() {
        return queueMetrics.getFailedJobCount(jobQueueName);
    }

    public long getExceptionCount() {
        return queueMetrics.getExceptionCount(jobQueueName);
    }

    public long getRunningJobsCount() {
        return queueMetrics.getRunningJobsCount(jobQueueName);
    }

    public long getPendingJobsCount() {
        return queueMetrics.getPendingJobsCount(jobQueueName);
    }

    public long getHighPriorityJobsCount() {
        return queueMetrics.getHighPriorityJobsCount(jobQueueName);
    }

    public long getMidPriorityJobsCount() {
        return queueMetrics.getMidPriorityJobsCount(jobQueueName);
    }

    public long getLowPriorityJobsCount() {
        return queueMetrics.getLowPriorityJobsCount(jobQueueName);
    }

}
