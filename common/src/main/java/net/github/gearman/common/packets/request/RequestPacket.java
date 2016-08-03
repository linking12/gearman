package net.github.gearman.common.packets.request;

import net.github.gearman.common.packets.Packet;
import net.github.gearman.constants.PacketMagic;

public abstract class RequestPacket extends Packet {

    public RequestPacket(){
    }

    public RequestPacket(byte[] fromdata){
        super(fromdata);
    }

    @Override
    public byte[] getMagic() {
        return PacketMagic.REQUEST;
    }

}
