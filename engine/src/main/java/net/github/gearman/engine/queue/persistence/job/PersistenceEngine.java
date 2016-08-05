package net.github.gearman.engine.queue.persistence.job;

import java.util.Collection;

import net.github.gearman.common.Job;
import net.github.gearman.engine.core.QueuedJob;

public interface PersistenceEngine {

    public String getIdentifier();

    public boolean write(Job job);

    public void delete(Job job);

    public void delete(String functionName, String uniqueID);

    public void deleteAll();

    public Job findJob(String functionName, String uniqueID);

    public Collection<QueuedJob> readAll();

    public Collection<QueuedJob> getAllForFunction(String functionName);
}
