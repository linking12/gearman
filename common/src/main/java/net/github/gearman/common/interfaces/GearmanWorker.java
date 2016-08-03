package net.github.gearman.common.interfaces;

import java.io.IOException;

import net.github.gearman.common.Job;

/**
 * The client-side gearman worker interface.
 */
public interface GearmanWorker {

    void registerCallback(String method, GearmanFunction function);

    void doWork();

    void stopWork();

    void sendData(Job job, byte[] data) throws IOException;

    void sendStatus(Job job, int numerator, int denominator) throws IOException;

    void sendWarning(Job job, byte[] warning) throws IOException;
}
