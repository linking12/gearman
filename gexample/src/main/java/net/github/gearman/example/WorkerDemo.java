package net.github.gearman.example;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.github.gearman.client.NetworkGearmanWorker;
import net.github.gearman.common.Job;
import net.github.gearman.common.events.WorkEvent;
import net.github.gearman.common.interfaces.GearmanFunction;
import net.github.gearman.net.Connection;

public class WorkerDemo {

    private static Logger LOG = LoggerFactory.getLogger(WorkerDemo.class);

    static class ReverseFunction implements GearmanFunction {

        @Override
        public byte[] process(WorkEvent workEvent) {
            Job job = workEvent.job;
            byte[] data = job.getData();
            String function = job.getFunctionName();
            LOG.debug("Got data for function " + function);
            ArrayUtils.reverse(data);
            return data;

        }
    }

    public static void main(String... args) {
        try {
            byte data[] = "This is a test".getBytes();
            NetworkGearmanWorker worker = new NetworkGearmanWorker.Builder().withConnection(new Connection("localhost",
                                                                                                           4730)).build();

            worker.registerCallback("reverse", new ReverseFunction());

            worker.doWork();
        } catch (Exception e) {
            LOG.error("oops!");
        }
    }
}
