package net.github.gearman.engine.core;

import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.jetty.util.ConcurrentHashSet;

import net.github.gearman.common.interfaces.EngineWorker;

public class WorkerPool {

    private final ConcurrentHashSet<EngineWorker> sleepingWorkers;
    private final ConcurrentHashSet<EngineWorker> connectedWorkers;
    private final AtomicLong                      numberOfConnectedWorkers;

    public WorkerPool(final String name){
        sleepingWorkers = new ConcurrentHashSet<>();
        connectedWorkers = new ConcurrentHashSet<>();
        numberOfConnectedWorkers = new AtomicLong(0);
    }

    public void addWorker(final EngineWorker worker) {
        connectedWorkers.add(worker);
        numberOfConnectedWorkers.incrementAndGet();
    }

    public void removeWorker(final EngineWorker worker) {
        sleepingWorkers.remove(worker);
        connectedWorkers.remove(worker);
        numberOfConnectedWorkers.decrementAndGet();
    }

    public void markSleeping(final EngineWorker worker) {
        sleepingWorkers.add(worker);
        worker.markAsleep();
    }

    public void wakeupWorkers() {
        for (final EngineWorker w : sleepingWorkers) {
            w.wakeUp();
        }
    }

    public void markAwake(final EngineWorker worker) {
        sleepingWorkers.remove(worker);
    }

    public long getNumberOfConnectedWorkers() {
        return numberOfConnectedWorkers.longValue();
    }
}
