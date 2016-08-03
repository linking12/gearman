package net.github.gearman.common.packets.request;

import net.github.gearman.constants.PacketType;

public class ResetAbilities extends RequestPacket {

    public ResetAbilities(){
        this.type = PacketType.RESET_ABILITIES;
    }

    public ResetAbilities(byte[] pktdata){
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
