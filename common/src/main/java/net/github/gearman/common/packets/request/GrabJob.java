package net.github.gearman.common.packets.request;

import net.github.gearman.constants.PacketType;

public class GrabJob extends RequestPacket {

    public GrabJob(){
        this.type = PacketType.GRAB_JOB;
    }

    public GrabJob(byte[] pktdata){
        super(pktdata);
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
