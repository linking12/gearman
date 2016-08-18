package net.github.gearman.server.web.dashboard;

import net.github.gearman.engine.metrics.QueueMetrics;
import net.github.gearman.server.util.SnapshottingJobQueueMonitor;

public class SystemStatusView extends StatusView {

    public SystemStatusView(SnapshottingJobQueueMonitor jobQueueMonitor, QueueMetrics queueMetrics)
    {
        super(jobQueueMonitor, queueMetrics);
    }
}
