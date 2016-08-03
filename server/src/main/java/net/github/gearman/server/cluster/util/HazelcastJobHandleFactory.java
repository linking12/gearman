package net.github.gearman.server.cluster.util;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;

import net.github.gearman.common.interfaces.JobHandleFactory;

public class HazelcastJobHandleFactory implements JobHandleFactory {

    @SuppressWarnings("unused")
    private final HazelcastInstance hazelcast;
    private final String            clusterHostname;
    final IAtomicLong               jobHandleCounter;

    public HazelcastJobHandleFactory(HazelcastInstance hazelcast, String clusterHostname){
        this.hazelcast = hazelcast;
        this.clusterHostname = clusterHostname;
        jobHandleCounter = hazelcast.getAtomicLong("jobhandlecounter");
    }

    public final byte[] getNextJobHandle() {
        String handle = "H:".concat(clusterHostname).concat(":").concat(String.valueOf(jobHandleCounter.incrementAndGet()));
        return handle.getBytes();
    }

}
