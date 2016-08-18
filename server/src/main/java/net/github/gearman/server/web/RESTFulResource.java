package net.github.gearman.server.web;

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.google.common.collect.Maps;
import com.google.inject.Inject;

import net.github.gearman.engine.metrics.QueueMetrics;
import net.github.gearman.server.config.GearmanServerConfiguration;
import net.github.gearman.server.config.ServerConfiguration;
import net.github.gearman.server.util.JobQueueMonitor;
import net.github.gearman.server.web.dashboard.StatusView;

@Path("/dashboard")
public class RESTFulResource {

    private final StatusView status;

    @Inject
    public RESTFulResource(final ServerConfiguration serverConfiguration){
        GearmanServerConfiguration gerverConfiguration = (GearmanServerConfiguration) serverConfiguration;
        JobQueueMonitor jobQueueMonitor = gerverConfiguration.getJobQueueMonitor();
        QueueMetrics queueMetrics = gerverConfiguration.getQueueMetrics();
        status = new StatusView(jobQueueMonitor, queueMetrics);
    }

    @GET
    @Path("/totalData")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Long> getHTMLData(@PathParam("name") String name) {
        Map<String, Long> dashboard = Maps.newHashMap();
        dashboard.put("totalJobsPending", status.getTotalJobsPending());
        dashboard.put("totalJobsProcessed", status.getTotalJobsProcessed());
        dashboard.put("totalJobsQueued", status.getTotalJobsQueued());
        dashboard.put("workerCount", status.getWorkerCount());
        return dashboard;
    }

}
