package net.github.gearman.common.client;

import java.util.HashSet;
import java.util.Set;

import net.github.gearman.common.JobStatus;
import net.github.gearman.common.events.GearmanClientEventListener;
import net.github.gearman.common.interfaces.GearmanClient;

public abstract class AbstractGearmanClient implements GearmanClient {

    protected final Set<GearmanClientEventListener> gearmanClientEventListenerSet;

    protected AbstractGearmanClient(){
        gearmanClientEventListenerSet = new HashSet<>();
    }

    @Override
    public void registerEventListener(GearmanClientEventListener listener) {
        gearmanClientEventListenerSet.add(listener);
    }

    protected void handleWorkData(final String jobHandle, final byte[] data) {
        for (GearmanClientEventListener el : gearmanClientEventListenerSet) {
            el.handleWorkData(jobHandle, data);
        }
    }

    protected void handleWorkWarning(final String jobHandle, final byte[] warning) {
        for (GearmanClientEventListener el : gearmanClientEventListenerSet) {
            el.handleWorkWarning(jobHandle, warning);
        }
    }

    protected void handleWorkStatus(final String jobHandle, final JobStatus jobStatus) {
        for (GearmanClientEventListener el : gearmanClientEventListenerSet) {
            el.handleWorkStatus(jobHandle, jobStatus);
        }
    }

}
