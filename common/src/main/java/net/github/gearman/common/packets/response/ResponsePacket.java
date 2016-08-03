package net.github.gearman.common.packets.response;

import net.github.gearman.common.packets.Packet;
import net.github.gearman.constants.PacketMagic;

public abstract class ResponsePacket extends Packet {

    public ResponsePacket(){
    }

    public ResponsePacket(byte[] fromdata){
        super(fromdata);
    }

    public byte[] getMagic() {
        return PacketMagic.RESPONSE;
    }
}
