package net.github.gearman.server.config;

import com.codahale.metrics.MetricRegistry;

import net.github.gearman.engine.exceptions.JobQueueFactoryException;
import net.github.gearman.engine.queue.factories.cronjob.CronJobQueueFactory;
import net.github.gearman.engine.queue.factories.cronjob.MysqlPersistedCronJobQueueFactory;
import net.github.gearman.server.config.persistence.DataBaseConfiguration;

public class CronJobPersistenceEngineConfiguration {

    private static final String   ENGINE_MYQL = "mysql";

    private DataBaseConfiguration dbSQL;

    private String                engine;
    private CronJobQueueFactory   cronJobQueueFactory;

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public DataBaseConfiguration getDbSQL() {
        return dbSQL;
    }

    public void setDbSQL(DataBaseConfiguration dbSQL) {
        this.dbSQL = dbSQL;
    }

    public CronJobQueueFactory getCronJobQueueFactory(MetricRegistry metricRegistry) {
        if (cronJobQueueFactory == null) {
            switch (getEngine()) {

                case ENGINE_MYQL:
                    try {
                        cronJobQueueFactory = new MysqlPersistedCronJobQueueFactory(dbSQL.getHost(), dbSQL.getPort(),
                                                                                    dbSQL.getDbName(), dbSQL.getUser(),
                                                                                    dbSQL.getPassword(),
                                                                                    dbSQL.getTable(), metricRegistry);
                    } catch (JobQueueFactoryException e) {
                        cronJobQueueFactory = null;
                    }
                    break;
                default:
                    cronJobQueueFactory = null;
            }
        }

        if (cronJobQueueFactory == null) {
            throw new RuntimeException("No cronjob queue factory was constructed, giving up!");
        }

        return cronJobQueueFactory;
    }
}
