package net.github.gearman.engine.util;

import java.util.concurrent.atomic.AtomicLong;

import net.github.gearman.engine.core.UniqueIdFactory;

public class LocalUniqueIdFactory implements UniqueIdFactory {

    private final AtomicLong uniqueIdCounter;

    public LocalUniqueIdFactory(){
        this.uniqueIdCounter = new AtomicLong(0L);
    }

    @Override
    public String generateUniqueId() {
        long uniqueId = uniqueIdCounter.incrementAndGet();
        return String.valueOf(uniqueId);
    }
}
