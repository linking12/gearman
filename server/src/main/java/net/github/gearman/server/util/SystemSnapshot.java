package net.github.gearman.server.util;

import java.util.Date;

public class SystemSnapshot {

    private final Date timestamp;
    private final Long totalJobsQueued;
    private final Long totalJobsProcessed;
    private final Long jobsQueuedSinceLastSnapshot;
    private final Long jobsProcessedSinceLastSnapshot;
    private final Long totalJobsPending;
    private final Long heapUsed;
    private final Long heapSize;

    public SystemSnapshot(Long totalJobsQueued, Long totalJobsProcessed, Long jobsQueuedSinceLastSnapshot,
                          Long jobsProcessedSinceLastSnapshot, Long totalJobsPending, Long heapSize, Long heapUsed){
        this.timestamp = new Date();
        this.totalJobsProcessed = totalJobsProcessed;
        this.totalJobsQueued = totalJobsQueued;
        this.jobsProcessedSinceLastSnapshot = jobsProcessedSinceLastSnapshot;
        this.jobsQueuedSinceLastSnapshot = jobsQueuedSinceLastSnapshot;
        this.totalJobsPending = totalJobsPending;
        this.heapUsed = heapUsed;
        this.heapSize = heapSize;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public Long getTotalJobsQueued() {
        return totalJobsQueued;
    }

    public Long getTotalJobsProcessed() {
        return totalJobsProcessed;
    }

    public Long getJobsQueuedSinceLastSnapshot() {
        return jobsQueuedSinceLastSnapshot;
    }

    public Long getJobsProcessedSinceLastSnapshot() {
        return jobsProcessedSinceLastSnapshot;
    }

    public Long getTotalJobsPending() {
        return totalJobsPending;
    }

    public Long getHeapUsed() {
        return heapUsed;
    }

    public Long getHeapSize() {
        return heapSize;
    }
}
