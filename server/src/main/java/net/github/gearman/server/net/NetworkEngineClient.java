package net.github.gearman.server.net;

import io.netty.channel.Channel;
import net.github.gearman.common.Job;
import net.github.gearman.common.JobStatus;
import net.github.gearman.common.interfaces.EngineClient;
import net.github.gearman.common.packets.Packet;
import net.github.gearman.common.packets.response.WorkCompleteResponse;
import net.github.gearman.common.packets.response.WorkDataResponse;
import net.github.gearman.common.packets.response.WorkExceptionResponse;
import net.github.gearman.common.packets.response.WorkFailResponse;
import net.github.gearman.common.packets.response.WorkStatus;
import net.github.gearman.common.packets.response.WorkWarningResponse;

public class NetworkEngineClient implements EngineClient {

    private final Channel channel;
    private Job           currentJob;

    public NetworkEngineClient(Channel channel){
        this.channel = channel;
    }

    @Override
    public Job getCurrentJob() {
        return currentJob;
    }

    @Override
    public void setCurrentJob(Job job) {
        this.currentJob = job;
    }

    @Override
    public void sendWorkResults(String jobHandle, byte[] data) {
        send(new WorkCompleteResponse(jobHandle, data));
    }

    @Override
    public void sendWorkData(String jobHandle, byte[] data) {
        send(new WorkDataResponse(jobHandle, data));
    }

    @Override
    public void sendWorkException(String jobHandle, byte[] exception) {
        send(new WorkExceptionResponse(jobHandle, exception));
    }

    @Override
    public void sendWorkFail(String jobHandle) {
        send(new WorkFailResponse(jobHandle));
    }

    @Override
    public void sendWorkWarning(String jobHandle, byte[] warning) {
        send(new WorkWarningResponse(jobHandle, warning));
    }

    @Override
    public void sendWorkStatus(JobStatus jobStatus) {
        send(new WorkStatus(jobStatus));
    }

    @Override
    public void send(Packet packet) {
        channel.writeAndFlush(packet);
    }

}
