package net.github.gearman.server.core;

import net.github.gearman.common.Job;

/**
 * Active job tracker
 */

public interface JobTracker {

    Job findByUniqueId(String uniqueId);

    Job findByJobHandle(String jobHandle);

}
