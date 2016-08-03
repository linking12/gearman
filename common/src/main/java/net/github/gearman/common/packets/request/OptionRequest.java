package net.github.gearman.common.packets.request;

import java.util.concurrent.atomic.AtomicReference;

import net.github.gearman.constants.PacketType;

public class OptionRequest extends RequestPacket {

    public AtomicReference<String> option;

    public OptionRequest(){
    }

    public OptionRequest(byte[] pktdata){
        super(pktdata);
        this.type = PacketType.OPTION_REQ;

        option = new AtomicReference<String>();

        int pOff = 0;
        pOff = parseString(pOff, option);
    }

    @Override
    public byte[] toByteArray() {
        byte[] optbytes = option.get().getBytes();
        byte[] result = this.concatByteArrays(getHeader(), optbytes);
        return result;
    }

    @Override
    public int getPayloadSize() {
        return this.option.get().length();
    }

    public String getOption() {
        return option.get();
    }
}
