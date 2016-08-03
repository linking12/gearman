package net.github.gearman.common.packets.request;

import net.github.gearman.constants.PacketType;

public class GrabJobUniq extends RequestPacket {

    public GrabJobUniq(){
        this.type = PacketType.GRAB_JOB_UNIQ;
    }

    public GrabJobUniq(byte[] pktdata){
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
