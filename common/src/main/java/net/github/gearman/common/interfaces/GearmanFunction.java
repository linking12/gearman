package net.github.gearman.common.interfaces;

import net.github.gearman.common.events.WorkEvent;

public interface GearmanFunction {

    public byte[] process(WorkEvent workEvent);
}
