package net.github.gearman.common.interfaces;

import java.util.Date;

import net.github.gearman.common.JobStatus;
import net.github.gearman.common.events.GearmanClientEventListener;
import net.github.gearman.constants.JobPriority;
import net.github.gearman.exceptions.JobSubmissionException;
import net.github.gearman.exceptions.WorkException;

public interface GearmanClient {

    String submitFutureJob(String callback, byte[] data, Date whenToRun) throws JobSubmissionException;

    String submitFutureJob(String callback, byte[] data, String cronExpression) throws JobSubmissionException;

    String submitJobInBackground(String callback, byte[] data) throws JobSubmissionException;

    String submitJobInBackground(String callback, byte[] data, JobPriority priority) throws JobSubmissionException;

    byte[] submitJob(String callback, byte[] data) throws JobSubmissionException, WorkException;

    byte[] submitJob(String callback, byte[] data, JobPriority priority) throws JobSubmissionException, WorkException;

    JobStatus getStatus(String jobHandle);

    void registerEventListener(GearmanClientEventListener listener);

    void shutdown();
}
