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
            for (int i = 0; i < 5; i++) {
                Runnable task = new WorkThread("reverse" + i);
                Thread workThread = new Thread(task);
                workThread.start();
            }
        } catch (Exception e) {
            LOG.error("oops!");
        }
    }

    static class WorkThread implements Runnable {

        private final String functionName;

        public WorkThread(String functionName){
            this.functionName = functionName;
        }

        @Override
        public void run() {
            NetworkGearmanWorker worker = new NetworkGearmanWorker.Builder().withConnection(new Connection("localhost",
                                                                                                           4730)).build();
            worker.registerCallback(functionName, new ReverseFunction());

            worker.doWork();

        }

    }
}
