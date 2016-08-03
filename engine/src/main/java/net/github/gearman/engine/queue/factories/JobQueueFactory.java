package net.github.gearman.engine.queue.factories;

import net.github.gearman.engine.core.QueuedJob;
import net.github.gearman.engine.exceptions.JobQueueFactoryException;
import net.github.gearman.engine.queue.JobQueue;

import java.util.Collection;

public interface JobQueueFactory {

    JobQueue build(String name) throws JobQueueFactoryException;

    Collection<QueuedJob> loadPersistedJobs();
}
