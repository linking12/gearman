package net.github.gearman.server.web;

import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import net.github.gearman.engine.metrics.QueueMetrics;
import net.github.gearman.server.config.GearmanServerConfiguration;
import net.github.gearman.server.config.ServerConfiguration;
import net.github.gearman.server.util.JobQueueMonitor;
import net.github.gearman.server.web.dashboard.JobQueueStatusView;
import net.github.gearman.server.web.dashboard.StatusView;

@Path("/dashboard")
public class RESTFulResource {

    private final StatusView      status;

    private final JobQueueMonitor jobQueueMonitor;
    private final QueueMetrics    queueMetrics;

    @Inject
    public RESTFulResource(final ServerConfiguration serverConfiguration){
        GearmanServerConfiguration gerverConfiguration = (GearmanServerConfiguration) serverConfiguration;
        jobQueueMonitor = gerverConfiguration.getJobQueueMonitor();
        queueMetrics = gerverConfiguration.getQueueMetrics();
        status = new StatusView(jobQueueMonitor, queueMetrics);
    }

    @GET
    @Path("/totalData")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> getTotalData(@PathParam("name") String name) {
        Map<String, Object> dashboard = Maps.newHashMap();
        dashboard.put("totalJobsPending", status.getTotalJobsPending());
        dashboard.put("totalJobsProcessed", status.getTotalJobsProcessed());
        dashboard.put("totalJobsQueued", status.getTotalJobsQueued());
        dashboard.put("workerCount", status.getWorkerCount());
        dashboard.put("maxHeapSize", status.getMaxHeapSize());
        dashboard.put("usedMemory", status.getUsedMemory());
        dashboard.put("heapSize", status.getHeapSize());
        dashboard.put("heapUsed", status.getHeapUsed());
        dashboard.put("memoryUsage", status.getMemoryUsage());
        dashboard.put("jobQueues", status.getJobQueues());
        return dashboard;
    }

    @GET
    @Path("/queues")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Map<String, Object>> getQueues() {
        List<Map<String, Object>> queues = Lists.newArrayList();
        List<String> queueNames = status.getJobQueues();
        for (String queueName : queueNames) {
            Map<String, Object> queue = Maps.newHashMap();
            JobQueueStatusView jobQueueStatusView = new JobQueueStatusView(jobQueueMonitor, queueMetrics, queueName);
            queue.put("Server", jobQueueStatusView.getHostname());
            queue.put("Function", queueName);
            queue.put("ActiveJob", jobQueueStatusView.getActiveJobCount());
            queue.put("EnqueuedJob", jobQueueStatusView.getEnqueuedJobCount());
            queue.put("CompletedJob", jobQueueStatusView.getCompletedJobCount());
            queue.put("FailedJob", jobQueueStatusView.getFailedJobCount());
            queue.put("Exception", jobQueueStatusView.getExceptionCount());
            queue.put("RunningJobs", jobQueueStatusView.getRunningJobsCount());
            queue.put("PendingJobs", jobQueueStatusView.getPendingJobsCount());
            queue.put("HighPriorityJobs", jobQueueStatusView.getHighPriorityJobsCount());
            queue.put("MidPriorityJobs", jobQueueStatusView.getMidPriorityJobsCount());
            queue.put("LowPriorityJobs", jobQueueStatusView.getLowPriorityJobsCount());
            queue.put("Worke Register", jobQueueStatusView.getWorkerCount(queueName));
            queues.add(queue);
        }
        return queues;
    }

}
