package net.github.gearman.engine.queue.factories.job;

import java.util.Collection;
import java.util.LinkedList;

import com.codahale.metrics.MetricRegistry;

import net.github.gearman.engine.core.JobManager;
import net.github.gearman.engine.core.QueuedJob;
import net.github.gearman.engine.exceptions.JobQueueFactoryException;
import net.github.gearman.engine.queue.JobQueue;
import net.github.gearman.engine.queue.PersistedJobQueue;
import net.github.gearman.engine.queue.persistence.job.MemoryPersistenceEngine;

public class MemoryJobQueueFactory implements JobQueueFactory {

    private final MetricRegistry metricRegistry;

    public MemoryJobQueueFactory(MetricRegistry metricRegistry){
        this.metricRegistry = metricRegistry;
    }

    public JobQueue build(String name) throws JobQueueFactoryException {
        return new PersistedJobQueue(name, new MemoryPersistenceEngine(), metricRegistry);
    }

    @Override
    public Collection<QueuedJob> loadPersistedJobs() {
        return new LinkedList<>();
    }

}
