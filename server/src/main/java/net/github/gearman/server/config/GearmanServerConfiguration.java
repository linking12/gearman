package net.github.gearman.server.config;

import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import net.github.gearman.common.interfaces.JobHandleFactory;
import net.github.gearman.engine.core.JobManager;
import net.github.gearman.engine.core.UniqueIdFactory;
import net.github.gearman.engine.metrics.MetricsEngine;
import net.github.gearman.engine.metrics.QueueMetrics;
import net.github.gearman.engine.queue.factories.cronjob.CronJobQueueFactory;
import net.github.gearman.engine.queue.factories.job.JobQueueFactory;
import net.github.gearman.engine.util.LocalJobHandleFactory;
import net.github.gearman.engine.util.LocalUniqueIdFactory;
import net.github.gearman.server.util.JobQueueMonitor;
import net.github.gearman.server.util.SnapshottingJobQueueMonitor;

public class GearmanServerConfiguration implements ServerConfiguration {

    private int                                   port;
    private int                                   httpPort;
    private boolean                               enableSSL;
    private boolean                               debugging;
    private String                                hostName;
    private CronJobQueueFactory                   cronJobQueueFactory;
    private JobQueueFactory                       jobQueueFactory;
    private JobManager                            jobManager;
    private JobQueueMonitor                       jobQueueMonitor;
    private JobPersistenceEngineConfiguration     jobPersistenceEngine;
    private CronJobPersistenceEngineConfiguration cronJobpersistenceEngine;
    private ExceptionStoreConfiguration           exceptionStorageEngine;
    private JobHandleFactory                      jobHandleFactory;
    private UniqueIdFactory                       uniqueIdFactory;
    private MetricRegistry                        metricRegistry;
    private QueueMetrics                          queueMetrics;
    private HealthCheckRegistry                   healthCheckRegistry;
    private Object                                configLock = new Object();

    public void setPort(int port) {
        this.port = port;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public boolean isEnableSSL() {
        return enableSSL;
    }

    public void setEnableSSL(boolean enableSSL) {
        this.enableSSL = enableSSL;
    }

    public void setDebugging(boolean debugging) {
        this.debugging = debugging;

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        if (debugging) {
            root.setLevel(Level.DEBUG);
        } else {
            root.setLevel(Level.ERROR);
        }
    }

    public JobPersistenceEngineConfiguration getJobPersistenceEngine() {
        return jobPersistenceEngine;
    }

    public void setJobPersistenceEngine(JobPersistenceEngineConfiguration jobPersistenceEngine) {
        this.jobPersistenceEngine = jobPersistenceEngine;
    }

    public CronJobPersistenceEngineConfiguration getCronJobpersistenceEngine() {
        return cronJobpersistenceEngine;
    }

    public void setCronJobpersistenceEngine(CronJobPersistenceEngineConfiguration cronJobpersistenceEngine) {
        this.cronJobpersistenceEngine = cronJobpersistenceEngine;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public void setJobQueueFactory(JobQueueFactory jobQueueFactory) {
        this.jobQueueFactory = jobQueueFactory;
    }

    public void setJobManager(JobManager jobManager) {
        this.jobManager = jobManager;
    }

    public void setJobQueueMonitor(SnapshottingJobQueueMonitor jobQueueMonitor) {
        this.jobQueueMonitor = jobQueueMonitor;
    }

    public ExceptionStoreConfiguration getExceptionStorageEngine() {
        return exceptionStorageEngine;
    }

    public void setExceptionStorageEngine(ExceptionStoreConfiguration exceptionStorageEngine) {
        this.exceptionStorageEngine = exceptionStorageEngine;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public int getHttpPort() {
        return httpPort;
    }

    @Override
    public boolean isSSLEnabled() {
        return enableSSL;
    }

    @Override
    public boolean isDebugging() {
        return debugging;
    }

    @Override
    public String getHostName() {
        if (hostName == null) {
            hostName = "localhost";
        }

        return hostName;
    }

    @Override
    public JobQueueFactory getJobQueueFactory() {
        if (jobQueueFactory == null && this.getJobPersistenceEngine() != null) {
            jobQueueFactory = this.getJobPersistenceEngine().getJobQueueFactory(getMetricRegistry());
        }

        return jobQueueFactory;
    }

    @Override
    public CronJobQueueFactory getCronJobQueueFactory() {
        if (cronJobQueueFactory == null && this.getCronJobpersistenceEngine() != null) {
            cronJobQueueFactory = this.getCronJobpersistenceEngine().getCronJobQueueFactory(getMetricRegistry());
        }

        return cronJobQueueFactory;
    }

    @Override
    public JobManager getJobManager() {
        if (jobManager == null) {
            jobManager = new JobManager(getJobQueueFactory(), getCronJobQueueFactory(), getJobHandleFactory(),
                                        getUniqueIdFactory(), getExceptionStorageEngine().getExceptionStorageEngine(), getQueueMetrics());
        }

        return jobManager;
    }

    @Override
    public JobQueueMonitor getJobQueueMonitor() {
        if (jobQueueMonitor == null) {
            jobQueueMonitor = new SnapshottingJobQueueMonitor(getQueueMetrics());
        }

        return jobQueueMonitor;
    }

    @Override
    public JobHandleFactory getJobHandleFactory() {
        if (jobHandleFactory == null) {
            jobHandleFactory = new LocalJobHandleFactory(getHostName());
        }

        return jobHandleFactory;
    }

    @Override
    public UniqueIdFactory getUniqueIdFactory() {
        if (uniqueIdFactory == null) {
            uniqueIdFactory = new LocalUniqueIdFactory();
        }

        return uniqueIdFactory;
    }

    @Override
    public MetricRegistry getMetricRegistry() {
        if (metricRegistry == null) {
            metricRegistry = new MetricRegistry();
        }
        return metricRegistry;
    }

    public QueueMetrics getQueueMetrics() {
        synchronized (configLock) {
            if (queueMetrics == null) {
                queueMetrics = new MetricsEngine(getMetricRegistry());
            }
        }
        return queueMetrics;
    }

    public HealthCheckRegistry getHealthCheckRegistry() {
        if (healthCheckRegistry == null) {
            healthCheckRegistry = new HealthCheckRegistry();
            if (jobPersistenceEngine != null) {
                HealthCheck dataStoreHealthcheck = jobPersistenceEngine.getHealthCheck();
                if (dataStoreHealthcheck != null) {
                    healthCheckRegistry.register(jobPersistenceEngine.getEngine(), dataStoreHealthcheck);
                }
            }
        }
        return healthCheckRegistry;
    }

}
