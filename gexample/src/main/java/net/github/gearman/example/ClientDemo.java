package net.github.gearman.example;

import java.io.IOException;

import net.github.gearman.client.NetworkGearmanClient;
import net.github.gearman.common.JobStatus;
import net.github.gearman.common.events.GearmanClientEventListener;
import net.github.gearman.exceptions.JobSubmissionException;

public class ClientDemo {

    private ClientDemo(){
    }

    public static void main(String... args) {
        GearmanClientEventListener eventListener = new GearmanClientEventListener() {

            @Override
            public void handleWorkData(String jobHandle, byte[] data) {
                System.err.println("Received data update for job " + jobHandle);
            }

            @Override
            public void handleWorkWarning(String jobHandle, byte[] warning) {
                System.err.println("Received warning for job " + jobHandle);
            }

            @Override
            public void handleWorkStatus(String jobHandle, JobStatus jobStatus) {
                System.err.println("Received status update for job " + jobHandle);
                System.err.println("Status: " + jobStatus.getNumerator() + " / " + jobStatus.getDenominator());
            }
        };

        try {
            byte data[] = "This is a test".getBytes();
            NetworkGearmanClient client = new NetworkGearmanClient("localhost", 4730);
            client.addHostToList("localhost", 4731);
            client.registerEventListener(eventListener);
            try {
                String cron = String.format("*/%s * * * * ?", 10);
                for (int i = 0; i < 5; i++) {
                    String result = client.submitFutureJob("reverse" + i, data, cron);
                    System.err.println("Result: " + new String(result));
                }
            } catch (JobSubmissionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (IOException ioe) {
            System.err.println("Couldn't connect: " + ioe);
        }
    }
}
