package net.github.gearman.client;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.github.gearman.common.Job;
import net.github.gearman.common.JobState;
import net.github.gearman.common.JobStatus;
import net.github.gearman.common.events.WorkEvent;
import net.github.gearman.common.interfaces.GearmanFunction;
import net.github.gearman.common.interfaces.GearmanWorker;
import net.github.gearman.common.packets.Packet;
import net.github.gearman.common.packets.request.CanDo;
import net.github.gearman.common.packets.request.GrabJob;
import net.github.gearman.common.packets.request.PreSleep;
import net.github.gearman.common.packets.response.JobAssign;
import net.github.gearman.common.packets.response.JobAssignUniq;
import net.github.gearman.common.packets.response.WorkCompleteResponse;
import net.github.gearman.common.packets.response.WorkDataResponse;
import net.github.gearman.common.packets.response.WorkExceptionResponse;
import net.github.gearman.common.packets.response.WorkStatus;
import net.github.gearman.common.packets.response.WorkWarningResponse;
import net.github.gearman.constants.PacketType;
import net.github.gearman.net.Connection;
import net.github.gearman.net.ConnectionPool;

public class NetworkGearmanWorker implements GearmanWorker, Runnable {

    private final ConnectionPool               connectionPool;
    private final Map<String, GearmanFunction> callbacks;
    private final AtomicBoolean                isActive;

    private static Logger                      LOG = LoggerFactory.getLogger(NetworkGearmanWorker.class);
    private final Map<Job, Connection>         jobConnectionMap;

    private NetworkGearmanWorker(){
        this.connectionPool = new ConnectionPool();
        this.callbacks = new HashMap<>();
        this.isActive = new AtomicBoolean(true);
        this.jobConnectionMap = new ConcurrentHashMap<>();
    }

    private NetworkGearmanWorker(NetworkGearmanWorker other){
        this.connectionPool = other.connectionPool;
        this.callbacks = other.callbacks;
        this.jobConnectionMap = other.jobConnectionMap;
        this.isActive = new AtomicBoolean(true);
    }

    @Override
    public void registerCallback(String method, GearmanFunction function) {
        callbacks.put(method, function);
        broadcastAbility(method);
    }

    @Override
    public void doWork() {
        while (isActive.get()) {
            for (Connection c : connectionPool.getGoodConnectionList()) {
                LOG.debug("Trying " + c.toString());
                Job nextJob = null;

                try {
                    c.sendPacket(new GrabJob());
                    Packet p = c.getNextPacket();

                    switch (p.getType()) {
                        case JOB_ASSIGN:
                            JobAssign jobAssign = (JobAssign) p;
                            nextJob = jobAssign.getJob();
                            break;
                        case JOB_ASSIGN_UNIQ:
                            JobAssignUniq jobAssignUniq = (JobAssignUniq) p;
                            nextJob = jobAssignUniq.getJob();
                            break;
                        case NO_JOB:
                            LOG.info("Worker sending PRE_SLEEP and sleeping for 30 seconds...");
                            c.sendPacket(new PreSleep());
                            try {
                                Packet noop = c.getNextPacket(30 * 1000);
                                if (noop.getType() != PacketType.NOOP) {
                                    LOG.error("Received invalid packet. Expected NOOP, received " + noop.getType());
                                }
                            } catch (SocketTimeoutException e) {
                                LOG.warn("Socket timed out waiting for next packet...");
                            }
                            break;
                    }

                    if (nextJob != null) {
                        jobConnectionMap.put(nextJob, c);
                        WorkEvent workEvent = new WorkEvent(nextJob, this);
                        try {
                            GearmanFunction function = callbacks.get(nextJob.getFunctionName());
                            byte[] result = function.process(workEvent);
                            c.sendPacket(new WorkCompleteResponse(nextJob.getJobHandle(), result));
                        } catch (Throwable e) {
                            StringWriter sw = new StringWriter();
                            PrintWriter pw = new PrintWriter(sw);
                            e.printStackTrace(pw);
                            c.sendPacket(new WorkExceptionResponse(nextJob.getJobHandle(),
                                                                   sw.getBuffer().toString().getBytes()));
                        }
                    }

                } catch (IOException ioe) {
                    LOG.error("I/O error: ", ioe);
                } finally {
                    if (nextJob != null) jobConnectionMap.remove(nextJob);
                }

            }
        }
        LOG.debug("Worker has been stopped.");
    }

    @Override
    public void stopWork() {
        LOG.debug("Stopping network gearman worker.");
        isActive.set(false);
        this.connectionPool.shutdown();
    }

    @Override
    public void sendData(Job job, byte[] data) throws IOException {
        WorkDataResponse workDataResponse = new WorkWarningResponse(job.getJobHandle(), data);
        Connection c = jobConnectionMap.get(job);
        c.sendPacket(workDataResponse);
        LOG.debug("Sent data with " + data.length + " bytes to " + c);
    }

    @Override
    public void sendStatus(Job job, int numerator, int denominator) throws IOException {
        JobStatus jobStatus = new JobStatus(numerator, denominator, JobState.WORKING, job.getJobHandle());
        Connection c = jobConnectionMap.get(job);
        c.sendPacket(new WorkStatus(jobStatus));
        LOG.debug("Sent work status of " + jobStatus + " to " + c);

    }

    @Override
    public void sendWarning(Job job, byte[] warning) throws IOException {
        WorkWarningResponse workWarningResponse = new WorkWarningResponse(job.getJobHandle(), warning);
        Connection c = jobConnectionMap.get(job);
        c.sendPacket(workWarningResponse);
        LOG.debug("Sent warning with " + warning.length + " bytes to " + c);
    }

    private void broadcastAbility(String functionName) {
        for (Connection c : connectionPool.getGoodConnectionList()) {
            try {
                c.sendPacket(new CanDo(functionName));
            } catch (IOException e) {
                LOG.error("IO Exception: ", e);
            }
        }
    }

    @Override
    public void run() {
        // To change body of implemented methods use File | Settings | File Templates.
    }

    public static class Builder {

        private NetworkGearmanWorker worker;

        public Builder(){
            this.worker = new NetworkGearmanWorker();
        }

        public NetworkGearmanWorker build() {
            return new NetworkGearmanWorker(worker);
        }

        public Builder withConnection(Connection c) {
            worker.connectionPool.addConnection(c);
            return this;
        }

        public Builder withHostPort(String host, int port) {
            worker.connectionPool.addHostPort(host, port);
            return this;
        }
    }

}
