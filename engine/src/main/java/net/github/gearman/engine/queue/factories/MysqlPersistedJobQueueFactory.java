package net.github.gearman.engine.queue.factories;

import java.sql.SQLException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

import net.github.gearman.engine.core.QueuedJob;
import net.github.gearman.engine.exceptions.JobQueueFactoryException;
import net.github.gearman.engine.queue.JobQueue;
import net.github.gearman.engine.queue.PersistedJobQueue;
import net.github.gearman.engine.queue.persistence.job.MysqlPersistenceEngine;

public class MysqlPersistedJobQueueFactory implements JobQueueFactory {

    private static Logger                LOG = LoggerFactory.getLogger(PostgreSQLPersistedJobQueueFactory.class);

    private final MysqlPersistenceEngine mysqlQueue;;
    private final MetricRegistry         metricRegistry;

    public MysqlPersistedJobQueueFactory(String hostname, int port, String database, String user, String password,
                                         String tableName,
                                         MetricRegistry metricRegistry) throws JobQueueFactoryException{
        try {
            this.metricRegistry = metricRegistry;
            this.mysqlQueue = new MysqlPersistenceEngine(hostname, port, database, user, password, tableName,
                                                         metricRegistry);
        } catch (SQLException e) {
            LOG.error("Unable to create mysql persistence engine: ", e);
            throw new JobQueueFactoryException("Could not create the PostgreSQL persistence engine!");
        }
    }

    @Override
    public JobQueue build(String name) throws JobQueueFactoryException {
        return new PersistedJobQueue(name, mysqlQueue, metricRegistry);
    }

    @Override
    public Collection<QueuedJob> loadPersistedJobs() {
        return this.mysqlQueue.readAll();
    }

}
