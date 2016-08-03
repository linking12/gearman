package net.github.gearman.common.events;

import net.github.gearman.common.Job;
import net.github.gearman.common.interfaces.GearmanWorker;

public class WorkEvent {

    public final Job           job;
    public final GearmanWorker worker;

    public WorkEvent(final Job job, final GearmanWorker worker){
        this.job = job;
        this.worker = worker;
    }
}
