package net.github.gearman.engine.core.cronjob;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import net.github.gearman.common.Job;
import net.github.gearman.engine.core.JobManager;
import net.github.gearman.engine.exceptions.InitException;

public class CronJob extends Job {

    private JobManager        jobManage;

    private final String      cronExpression;

    /** 定时Job详情 */
    private final JobDetail   jobDetail;

    /** 定时触发器 */
    private final CronTrigger cronTrigger;

    public CronJob(String cronExpression, Job job){
        this.jobDetail = JobBuilder.newJob(TimerExecutor.class).withIdentity(this.getUniqueID()).build();
        this.cronTrigger = TriggerBuilder.newTrigger().withIdentity(this.getUniqueID()).withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build();
        this.cronExpression = cronExpression;
        this.cloneOtherJob(job);
    }

    public JobManager getJobManage() {
        return jobManage;
    }

    public void setJobManage(JobManager jobManage) {
        this.jobManage = jobManage;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void init() throws InitException {
        if (jobManage == null) {
            throw new InitException("[InternalJob]: init timer job error, JobManager is null,job:" + this.toString());
        }
        try {
            Scheduler scheduler = SchedulerFactoryHolder.getSchedulerFactory().getScheduler();
            scheduler.scheduleJob(jobDetail, cronTrigger);
            scheduler.getContext().put(jobDetail.getKey().getName(), this);
            scheduler.start();
        } catch (Throwable e) {
            throw new InitException("[InternalJob]: init timer job error, job:" + this.toString(), e);
        }
    }

    private static final class SchedulerFactoryHolder {

        private static class LazyHolder {

            private static final StdSchedulerFactory INSTANCE = new StdSchedulerFactory();
        }

        private SchedulerFactoryHolder(){
        }

        public static final StdSchedulerFactory getSchedulerFactory() {
            return LazyHolder.INSTANCE;
        }
    }

}
