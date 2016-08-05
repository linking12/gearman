package net.github.gearman.server.config;

import com.codahale.metrics.MetricRegistry;

import net.github.gearman.common.interfaces.JobHandleFactory;
import net.github.gearman.engine.core.JobManager;
import net.github.gearman.engine.core.UniqueIdFactory;
import net.github.gearman.engine.queue.factories.cronjob.CronJobQueueFactory;
import net.github.gearman.engine.queue.factories.job.JobQueueFactory;
import net.github.gearman.server.util.JobQueueMonitor;

public interface ServerConfiguration {

    int getPort();

    int getHttpPort();

    boolean isSSLEnabled();

    boolean isDebugging();

    String getHostName();

    JobQueueFactory getJobQueueFactory();

    CronJobQueueFactory getCronJobQueueFactory();

    JobManager getJobManager();

    JobQueueMonitor getJobQueueMonitor();

    JobHandleFactory getJobHandleFactory();

    UniqueIdFactory getUniqueIdFactory();

    MetricRegistry getMetricRegistry();

}
