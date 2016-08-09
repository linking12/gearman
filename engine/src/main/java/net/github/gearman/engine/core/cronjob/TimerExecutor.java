package net.github.gearman.engine.core.cronjob;

import java.util.concurrent.atomic.AtomicInteger;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.github.gearman.engine.core.JobManager;
import net.github.gearman.engine.queue.JobQueue;

/**
 * 定时执行器,抽象类;
 */
public class TimerExecutor implements org.quartz.Job {

    private static Logger LOG    = LoggerFactory.getLogger(TimerExecutor.class);

    private AtomicInteger number = new AtomicInteger(0);

    /**
     * 执行入口
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        CronJob job = null;
        try {
            job = (CronJob) context.getScheduler().getContext().get(context.getJobDetail().getKey().getName());
            JobManager jobManager = job.getJobManage();
            JobQueue jobQueue = jobManager.getOrCreateJobQueue(job.getFunctionName());
            int jobId = number.getAndIncrement();
            job.setUniqueID(job.getUniqueID() + "-" + jobId);
            jobQueue.enqueue(job);
            LOG.info("[TimerExecutor]: jobId:" + job.getUniqueID() + ", fireTime:"
                     + context.getFireTime().toLocaleString() + " reenqueue ");
        } catch (Throwable e) {
            LOG.error("[TimerExecutor]: execute error" + ", jobId:" + context.getJobDetail().getKey().getName()
                      + ", fireTime:" + context.getFireTime().toLocaleString(), e);
        }

    }

}
