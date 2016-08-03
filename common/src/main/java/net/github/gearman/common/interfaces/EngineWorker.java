package net.github.gearman.common.interfaces;

import java.util.Set;

public interface EngineWorker {

    public Set<String> getAbilities();

    public void wakeUp();

    public void markAsleep();

}
