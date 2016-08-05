package net.github.gearman.server.config;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.codahale.metrics.MetricRegistry;

import net.github.gearman.common.interfaces.JobHandleFactory;
import net.github.gearman.engine.core.JobManager;
import net.github.gearman.engine.core.UniqueIdFactory;
import net.github.gearman.engine.queue.factories.job.JobQueueFactory;
import net.github.gearman.engine.queue.factories.job.MemoryJobQueueFactory;
import net.github.gearman.engine.storage.NoopExceptionStorageEngine;
import net.github.gearman.engine.util.LocalJobHandleFactory;
import net.github.gearman.engine.util.LocalUniqueIdFactory;
import net.github.gearman.server.util.JobQueueMonitor;
import net.github.gearman.server.util.SnapshottingJobQueueMonitor;

// Sane defaults.
public class DefaultServerConfiguration extends GearmanServerConfiguration {

    private final JobManager       jobManager;
    private final JobQueueFactory  jobQueueFactory;
    private final JobQueueMonitor  jobQueueMonitor;
    private final JobHandleFactory jobHandleFactory;
    private final UniqueIdFactory  uniqueIdFactory;
    private final MetricRegistry   registry;

    public DefaultServerConfiguration(){
        this.registry = new MetricRegistry();
        this.jobHandleFactory = new LocalJobHandleFactory(getHostName());
        this.jobQueueFactory = new MemoryJobQueueFactory(registry);
        this.uniqueIdFactory = new LocalUniqueIdFactory();
        this.jobManager = new JobManager(jobQueueFactory, getCronJobQueueFactory(), jobHandleFactory, uniqueIdFactory,
                                         new NoopExceptionStorageEngine(), getQueueMetrics());
        this.jobQueueMonitor = new SnapshottingJobQueueMonitor(getQueueMetrics());
    }

    @Override
    public int getPort() {
        return 4730;
    }

    @Override
    public int getHttpPort() {
        return 8080;
    }

    @Override
    public boolean isSSLEnabled() {
        return false;
    }

    @Override
    public boolean isDebugging() {
        return false;
    }

    @Override
    public String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

    @Override
    public JobQueueFactory getJobQueueFactory() {
        return jobQueueFactory;
    }

    @Override
    public JobManager getJobManager() {
        return jobManager;
    }

    @Override
    public JobQueueMonitor getJobQueueMonitor() {
        return jobQueueMonitor;
    }

    @Override
    public JobHandleFactory getJobHandleFactory() {
        return jobHandleFactory;
    }

    @Override
    public UniqueIdFactory getUniqueIdFactory() {
        return uniqueIdFactory;
    }
}
