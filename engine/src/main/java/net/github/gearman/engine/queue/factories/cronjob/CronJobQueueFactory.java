package net.github.gearman.engine.queue.factories.cronjob;

import net.github.gearman.engine.core.JobManager;
import net.github.gearman.engine.queue.persistence.cronjob.CronJobPersistenceEngine;

public interface CronJobQueueFactory {

    void triggerCronJob(JobManager jobmanager);

    CronJobPersistenceEngine getCronJobPersistenceEngine();
}
