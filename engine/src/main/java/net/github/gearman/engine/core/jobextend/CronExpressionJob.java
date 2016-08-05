package net.github.gearman.engine.core.jobextend;

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

public class CronExpressionJob extends Job {

    /** 定时Job详情 */
    private final JobDetail   jobDetail;

    /** 定时触发器 */
    private final CronTrigger cronTrigger;

    private final JobManager  jobManage;

    public CronExpressionJob(String cronExpression, JobManager jobManage){
        this.jobDetail = JobBuilder.newJob(TimerExecutor.class).withIdentity(this.getUniqueID()).build();
        this.cronTrigger = TriggerBuilder.newTrigger().withIdentity(this.getUniqueID()).withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build();
        this.jobManage = jobManage;
    }

    public JobManager getJobManage() {
        return jobManage;
    }

    public void init() throws InitException {
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
