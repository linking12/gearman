package net.github.gearman.common.packets.response;

import net.github.gearman.constants.PacketType;

public interface WorkResponse {

    public abstract String getJobHandle();

    public abstract PacketType getType();
}
