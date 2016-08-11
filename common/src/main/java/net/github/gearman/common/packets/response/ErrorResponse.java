package net.github.gearman.common.packets.response;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import net.github.gearman.constants.GearmanConstants;
import net.github.gearman.constants.PacketType;

public class ErrorResponse extends ResponsePacket {

    private final AtomicReference<String> taskName, jobId;

    private final byte[]                  errorMessage;

    private final int                     size;

    public ErrorResponse(String function, String unique_id, byte[] errorMessage){
        this.type = PacketType.ERROR;
        this.taskName = new AtomicReference<>(function);
        this.jobId = new AtomicReference<>(unique_id);
        this.errorMessage = errorMessage.clone();
        this.size = function.length() + 1 + unique_id.length() + 1 + errorMessage.length;
    }

    public ErrorResponse(byte[] pktdata){
        super(pktdata);
        taskName = new AtomicReference<>();
        jobId = new AtomicReference<>();
        int pOff = 0;
        pOff = parseString(pOff, taskName);
        pOff = parseString(pOff, jobId);
        errorMessage = Arrays.copyOfRange(rawdata, pOff, rawdata.length);
        this.size = rawdata.length;
        this.type = PacketType.ERROR;
    }

    @Override
    public byte[] toByteArray() {
        byte[] metadata = stringsToTerminatedByteArray(taskName.get(), jobId.get());
        return concatByteArrays(getHeader(), metadata, errorMessage);
    }

    @Override
    public int getPayloadSize() {
        return size;
    }

    public String getErrorMessage() {
        return new String(errorMessage, GearmanConstants.CHARSET);
    }

}
