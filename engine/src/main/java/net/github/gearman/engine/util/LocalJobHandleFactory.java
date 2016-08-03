package net.github.gearman.engine.util;

import java.util.concurrent.atomic.AtomicLong;

import net.github.gearman.common.interfaces.JobHandleFactory;

public class LocalJobHandleFactory implements JobHandleFactory {

    private final AtomicLong jobHandleCounter;
    private final String     hostName;

    public LocalJobHandleFactory(String hostName){
        this.hostName = hostName;
        this.jobHandleCounter = new AtomicLong(0L);
    }

    /**
     * Returns the next available job handle
     * 
     * @return the next available job handle
     */
    public byte[] getNextJobHandle() {
        String handle = "H:".concat(hostName).concat(":").concat(String.valueOf(jobHandleCounter.incrementAndGet()));
        return handle.getBytes();
    }
}
