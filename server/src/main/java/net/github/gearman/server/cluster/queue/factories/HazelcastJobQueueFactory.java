package net.github.gearman.server.cluster.queue.factories;

import java.util.Collection;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;

import net.github.gearman.engine.core.QueuedJob;
import net.github.gearman.engine.exceptions.JobQueueFactoryException;
import net.github.gearman.engine.queue.factories.JobQueueFactory;
import net.github.gearman.server.cluster.queue.HazelcastJobQueue;

public class HazelcastJobQueueFactory implements JobQueueFactory {

    private final Logger            LOG = LoggerFactory.getLogger(HazelcastJobQueueFactory.class);

    private final HazelcastInstance hazelcast;

    public HazelcastJobQueueFactory(HazelcastInstance hazelcast){
        LOG.debug("Starting HazelcastJobQueueFactory");
        this.hazelcast = hazelcast;
    }

    public HazelcastJobQueue build(String name) throws JobQueueFactoryException {
        return new HazelcastJobQueue(name, hazelcast);
    }

    @Override
    public Collection<QueuedJob> loadPersistedJobs() {
        return new HashSet<>();
    }
}
