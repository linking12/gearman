package net.github.gearman.common.packets.response;

import java.util.concurrent.atomic.AtomicReference;

import net.github.gearman.constants.PacketType;

public class JobCreated extends ResponsePacket {

    public AtomicReference<String> jobHandle;

    public JobCreated(byte[] pktdata){
        super(pktdata);
        jobHandle = new AtomicReference<>();
        parseString(0, jobHandle);
        this.type = PacketType.JOB_CREATED;
    }

    public JobCreated(String jobhandle){
        jobHandle = new AtomicReference<>(jobhandle);
        this.type = PacketType.JOB_CREATED;
    }

    public String getJobHandle() {
        return this.jobHandle.get();
    }

    @Override
    public byte[] toByteArray() {
        return concatByteArrays(getHeader(), jobHandle.get().getBytes());
    }

    @Override
    public int getPayloadSize() {
        return this.jobHandle.get().length();
    }
}
