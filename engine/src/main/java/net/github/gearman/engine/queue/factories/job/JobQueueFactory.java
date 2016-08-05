package net.github.gearman.engine.queue.factories.job;

import java.util.Collection;

import net.github.gearman.engine.core.QueuedJob;
import net.github.gearman.engine.exceptions.JobQueueFactoryException;
import net.github.gearman.engine.queue.JobQueue;

public interface JobQueueFactory {

    JobQueue build(String name) throws JobQueueFactoryException;

    Collection<QueuedJob> loadPersistedJobs();

}
