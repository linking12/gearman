package net.github.gearman.common.packets.request;

import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import org.quartz.CronExpression;

import net.github.gearman.constants.JobPriority;
import net.github.gearman.constants.PacketType;

public class SubmitJob extends RequestPacket {

    private final AtomicReference<String> taskName, uniqueId, epochString;
    private final byte[]                  data;
    private final boolean                 background;
    private final int                     size;

    public SubmitJob(byte[] pktdata){
        super(pktdata);
        taskName = new AtomicReference<>();
        uniqueId = new AtomicReference<>();
        epochString = new AtomicReference<>();

        int pOff = 0;

        pOff = parseString(pOff, taskName);
        pOff = parseString(pOff, uniqueId);

        if (this.type == PacketType.SUBMIT_JOB_EPOCH) {
            pOff = parseString(pOff, epochString);
        }

        this.background = this.type == PacketType.SUBMIT_JOB_HIGH_BG || this.type == PacketType.SUBMIT_JOB_LOW_BG
                          || this.type == PacketType.SUBMIT_JOB_BG || this.type == PacketType.SUBMIT_JOB_EPOCH;

        data = Arrays.copyOfRange(rawdata, pOff, rawdata.length);
        this.size = rawdata.length;
    }

    public SubmitJob(String function, String unique_id, byte[] data, boolean background){
        this(function, unique_id, data, background, JobPriority.NORMAL);
    }

    public SubmitJob(String function, String unique_id, byte[] data, boolean background, JobPriority priority){
        this(function, unique_id, data, background, priority, null);
    }

    public SubmitJob(String function, String uniqueID, byte[] data, Date when){
        this(function, uniqueID, data, false, JobPriority.NORMAL, when);
    }

    public SubmitJob(String function, String uniqueID, byte[] data, String cronExpression){
        this(function, uniqueID, data, false, JobPriority.NORMAL, cronExpression);
    }

    public SubmitJob(String function, String unique_id, byte[] data, boolean background, JobPriority priority,
                     Object when){
        this.taskName = new AtomicReference<>(function);
        this.uniqueId = new AtomicReference<>(unique_id);
        this.epochString = new AtomicReference<>();
        this.background = background;
        this.data = data.clone();

        switch (priority) {
            case HIGH:
                this.type = background ? PacketType.SUBMIT_JOB_HIGH_BG : PacketType.SUBMIT_JOB_HIGH;
                break;
            case NORMAL:
                this.type = background ? PacketType.SUBMIT_JOB_BG : PacketType.SUBMIT_JOB;
                break;
            case LOW:
                this.type = background ? PacketType.SUBMIT_JOB_LOW_BG : PacketType.SUBMIT_JOB_LOW;
                break;
            default:
                break;
        }
        if (when == null) {
            this.size = function.length() + 1 + unique_id.length() + 1 + data.length;
        } else {
            this.type = PacketType.SUBMIT_JOB_EPOCH;
            if (when instanceof Date) {
                Date tempWhen = (Date) when;
                this.epochString.set(String.valueOf(tempWhen.getTime()));
            } else if (when instanceof String) {
                String tempWhen = ((String) when).trim();
                if (!CronExpression.isValidExpression(tempWhen)) {
                    throw new IllegalArgumentException("error cronExpression " + tempWhen);
                }
                this.epochString.set(tempWhen);
            }
            this.size = function.length() + 1 + unique_id.length() + 1 + epochString.get().length() + 1 + data.length;
        }
    }

    public Date getWhen() {
        return new Date(Long.parseLong(epochString.get()));
    }

    @SuppressWarnings("incomplete-switch")
    public JobPriority getPriority() {
        switch (this.type) {
            case SUBMIT_JOB:
            case SUBMIT_JOB_BG:
            case SUBMIT_JOB_EPOCH:
            case SUBMIT_JOB_SCHED:
                return JobPriority.NORMAL;
            case SUBMIT_JOB_HIGH:
            case SUBMIT_JOB_HIGH_BG:
                return JobPriority.HIGH;
            case SUBMIT_JOB_LOW:
            case SUBMIT_JOB_LOW_BG:
                return JobPriority.LOW;
        }

        return null;
    }

    public String getFunctionName() {
        return this.taskName.get();
    }

    public String getUniqueId() {
        return uniqueId.get();
    }

    public boolean isBackground() {
        return background;
    }

    public byte[] getData() {
        return data;
    }

    public String getEpoch() {
        return epochString.get();
    }

    @Override
    public byte[] toByteArray() {

        byte[] metadata;
        if (type == PacketType.SUBMIT_JOB_EPOCH) {
            metadata = stringsToTerminatedByteArray(taskName.get(), uniqueId.get(), epochString.get());
        } else {
            metadata = stringsToTerminatedByteArray(taskName.get(), uniqueId.get());
        }

        return concatByteArrays(getHeader(), metadata, data);
    }

    @Override
    public int getPayloadSize() {
        return size;
    }

}
