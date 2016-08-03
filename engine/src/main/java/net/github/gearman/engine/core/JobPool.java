package net.github.gearman.engine.core;

import java.util.Set;

import net.github.gearman.common.Job;
import net.github.gearman.common.interfaces.EngineClient;

/**
 * Pool of currently running work
 */
public interface JobPool {

    void addJob(Job job);

    void addClientForUniqueId(String uniqueId, EngineClient client);

    void removeClientForUniqueId(String uniqueId, EngineClient client);

    Set<EngineClient> clientsForUniqueId(String uniqueId);

    Job getJobByJobHandle(String jobHandle);

    Job getJobByUniqueId(String uniqueId);

}
