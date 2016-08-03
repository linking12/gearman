package net.github.gearman.common.packets.response;

import net.github.gearman.constants.PacketType;

public class OptionResponse extends ResponsePacket {

    private final byte[] data;

    public OptionResponse(String option){
        this.type = PacketType.OPTION_RES;
        this.data = option.getBytes();
    }

    @Override
    public byte[] toByteArray() {
        byte[] result = concatByteArrays(getHeader(), data);
        return result;
    }

    @Override
    public int getPayloadSize() {
        return data.length;
    }

    public byte[] getData() {
        return data;
    }
}
