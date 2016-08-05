package net.github.gearman.engine.queue.factories;

import com.codahale.metrics.MetricRegistry;

import net.github.gearman.engine.core.QueuedJob;
import net.github.gearman.engine.exceptions.JobQueueFactoryException;
import net.github.gearman.engine.queue.JobQueue;
import net.github.gearman.engine.queue.PersistedJobQueue;
import net.github.gearman.engine.queue.persistence.job.RedisPersistenceEngine;

import java.util.Collection;

public class RedisPersistedJobQueueFactory implements JobQueueFactory {

    private final RedisPersistenceEngine redisQueue;
    private final MetricRegistry         metricRegistry;

    public RedisPersistedJobQueueFactory(final String redisHostname, final int redisPort,
                                         MetricRegistry metricRegistry){
        this.metricRegistry = metricRegistry;
        this.redisQueue = new RedisPersistenceEngine(redisHostname, redisPort, metricRegistry);
    }

    @Override
    public JobQueue build(String name) throws JobQueueFactoryException {
        return new PersistedJobQueue(name, redisQueue, metricRegistry);
    }

    @Override
    public Collection<QueuedJob> loadPersistedJobs() {
        return redisQueue.readAll();
    }
}
