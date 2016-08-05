package net.github.gearman.server.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;

import net.github.gearman.engine.exceptions.JobQueueFactoryException;
import net.github.gearman.engine.healthchecks.RedisHealthCheck;
import net.github.gearman.engine.queue.factories.job.JobQueueFactory;
import net.github.gearman.engine.queue.factories.job.MemoryJobQueueFactory;
import net.github.gearman.engine.queue.factories.job.MysqlPersistedJobQueueFactory;
import net.github.gearman.engine.queue.factories.job.PostgreSQLPersistedJobQueueFactory;
import net.github.gearman.engine.queue.factories.job.RedisPersistedJobQueueFactory;
import net.github.gearman.server.config.persistence.DataBaseConfiguration;
import net.github.gearman.server.config.persistence.RedisConfiguration;
import redis.clients.jedis.Jedis;

public class JobPersistenceEngineConfiguration {

    private static final String   ENGINE_MEMORY   = "memory";
    private static final String   ENGINE_REDIS    = "redis";
    private static final String   ENGINE_POSTGRES = "postgres";
    private static final String   ENGINE_MYQL     = "mysql";

    private RedisConfiguration    redis;
    private DataBaseConfiguration dbSQL;

    private String                engine;
    private JobQueueFactory       jobQueueFactory;

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public RedisConfiguration getRedis() {
        return redis;
    }

    public void setRedis(RedisConfiguration redis) {
        this.redis = redis;
    }

    public DataBaseConfiguration getDbSQL() {
        return dbSQL;
    }

    public void setDbSQL(DataBaseConfiguration dbSQL) {
        this.dbSQL = dbSQL;
    }

    public JobQueueFactory getJobQueueFactory(MetricRegistry metricRegistry) {
        if (jobQueueFactory == null) {
            switch (getEngine()) {
                case ENGINE_MEMORY:
                    jobQueueFactory = new MemoryJobQueueFactory(metricRegistry);
                    break;
                case ENGINE_POSTGRES:
                    try {
                        jobQueueFactory = new PostgreSQLPersistedJobQueueFactory(dbSQL.getHost(), dbSQL.getPort(),
                                                                                 dbSQL.getDbName(), dbSQL.getUser(),
                                                                                 dbSQL.getPassword(), dbSQL.getTable(),
                                                                                 metricRegistry);
                    } catch (JobQueueFactoryException e) {
                        jobQueueFactory = null;
                    }
                    break;
                case ENGINE_MYQL:
                    try {
                        jobQueueFactory = new MysqlPersistedJobQueueFactory(dbSQL.getHost(), dbSQL.getPort(),
                                                                            dbSQL.getDbName(), dbSQL.getUser(),
                                                                            dbSQL.getPassword(), dbSQL.getTable(),
                                                                            metricRegistry);
                    } catch (JobQueueFactoryException e) {
                        jobQueueFactory = null;
                    }
                    break;
                case ENGINE_REDIS:
                    jobQueueFactory = new RedisPersistedJobQueueFactory(redis.getHost(), redis.getPort(),
                                                                        metricRegistry);
                    break;
                default:
                    jobQueueFactory = null;
            }
        }

        if (jobQueueFactory == null) {
            throw new RuntimeException("No job queue factory was constructed, giving up!");
        }

        return jobQueueFactory;
    }

    public HealthCheck getHealthCheck() {
        switch (getEngine()) {
            case ENGINE_REDIS:
                return new RedisHealthCheck(new Jedis(redis.getHost(), redis.getPort()));
            default:
                return null;
        }
    }
}
