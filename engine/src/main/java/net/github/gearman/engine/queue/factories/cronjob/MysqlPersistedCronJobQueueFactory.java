package net.github.gearman.engine.queue.factories.cronjob;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

import net.github.gearman.engine.core.JobManager;
import net.github.gearman.engine.core.cronjob.CronJob;
import net.github.gearman.engine.exceptions.InitException;
import net.github.gearman.engine.exceptions.JobQueueFactoryException;
import net.github.gearman.engine.queue.persistence.cronjob.CronJobPersistenceEngine;
import net.github.gearman.engine.queue.persistence.cronjob.MysqlCronJobPersistenceEngine;

public class MysqlPersistedCronJobQueueFactory implements CronJobQueueFactory {

    private static Logger                       LOG = LoggerFactory.getLogger(CronJobQueueFactory.class);

    private final MysqlCronJobPersistenceEngine mysqlCronJobPersistenceEngine;

    public MysqlPersistedCronJobQueueFactory(String hostname, int port, String database, String user, String password,
                                             String cronJobTableName,
                                             MetricRegistry metricRegistry) throws JobQueueFactoryException{
        try {
            this.mysqlCronJobPersistenceEngine = new MysqlCronJobPersistenceEngine(hostname, port, database, user,
                                                                                   password, cronJobTableName,
                                                                                   metricRegistry);
        } catch (SQLException e) {
            LOG.error("Unable to create mysql persistence engine: ", e);
            throw new JobQueueFactoryException("Could not create the PostgreSQL persistence engine!");
        }
    }

    @Override
    public void triggerCronJob(JobManager jobManage) {
        Collection<CronJob> jobs = mysqlCronJobPersistenceEngine.readAll();
        for (Iterator<CronJob> it = jobs.iterator(); it.hasNext();) {
            CronJob job = it.next();
            job.setJobManage(jobManage);
            try {
                job.init();
            } catch (InitException e) {
                LOG.error("Cronjob init failed,jobId:" + job.getUniqueID(), e);
            }
        }
    }

    @Override
    public CronJobPersistenceEngine getCronJobPersistenceEngine() {
        return mysqlCronJobPersistenceEngine;
    }

}
