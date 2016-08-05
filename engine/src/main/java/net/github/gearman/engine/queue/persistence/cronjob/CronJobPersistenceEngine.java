package net.github.gearman.engine.queue.persistence.cronjob;

import net.github.gearman.engine.core.cronjob.CronJob;

public interface CronJobPersistenceEngine {

    public boolean write(CronJob job);

    public void delete(CronJob job);

    public void delete(String functionName, String uniqueID);

    public void deleteAll();

    public CronJob findJob(String functionName, String uniqueID);
    

}
