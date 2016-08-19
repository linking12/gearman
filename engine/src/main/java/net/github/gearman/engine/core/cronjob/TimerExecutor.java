package net.github.gearman.engine.core.cronjob;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.github.gearman.common.Job;
import net.github.gearman.engine.core.JobManager;
import net.github.gearman.engine.queue.JobQueue;

/**
 * 定时执行器,抽象类;
 */
public class TimerExecutor implements org.quartz.Job {

    private static Logger LOG = LoggerFactory.getLogger(TimerExecutor.class);

    /**
     * 执行入口
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        CronJob job = null;
        try {
            job = (CronJob) context.getScheduler().getContext().get(context.getJobDetail().getKey().getName());
            JobManager jobManager = job.getJobManage();
            int order = job.getTimes().getAndIncrement();
            String jobId = substringBefore(job.getUniqueID(), "_").concat("_") + order;
            String jobHandler = substringBefore(job.getJobHandle(), "_").concat("_") + order;
            job.setUniqueID(jobId);
            job.setJobHandle(jobHandler);
            Job enqueueJob = new Job(job);
            jobManager.storeJob(enqueueJob);
            LOG.info("[TimerExecutor]: jobId:" + job.getUniqueID() + ", fireTime:"
                     + context.getFireTime().toLocaleString() + " reenqueue ");
        } catch (Throwable e) {
            LOG.error("[TimerExecutor]: execute error" + ", jobId:" + context.getJobDetail().getKey().getName()
                      + ", fireTime:" + context.getFireTime().toLocaleString(), e);
        }

    }

    private String substringBefore(final String str, final String separator) {
        if ((str == null || str.length() == 0) || separator == null) {
            return str;
        }
        if (separator.isEmpty()) {
            return "";
        }
        final int pos = str.indexOf(separator);
        if (pos == -1) {
            return str;
        }
        return str.substring(0, pos);
    }

}
