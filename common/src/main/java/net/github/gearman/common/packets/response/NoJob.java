package net.github.gearman.common.packets.response;

import net.github.gearman.constants.PacketType;

public class NoJob extends ResponsePacket {

    public NoJob(){
        this.type = PacketType.NO_JOB;
    }

    @Override
    public byte[] toByteArray() {
        return getHeader();
    }

    @Override
    public int getPayloadSize() {
        return 0;
    }
}
