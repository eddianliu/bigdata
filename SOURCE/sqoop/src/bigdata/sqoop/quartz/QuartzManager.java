package bigdata.sqoop.quartz;

import org.apache.log4j.Logger;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

/**
 * 
 * @author Liu.J
 *
 */
public class QuartzManager {

	private static SchedulerFactory factory = new StdSchedulerFactory();

	private static String JOB_GROUP_NAME = "SQOOP_JOB_GROUP";

	private static String TRIGGER_GROUP_NAME = "SQOOP_TRIGGER_GROUP";

	private static Logger log = Logger.getLogger(QuartzManager.class);

	/**
	 * 添加一个定时任务
	 * 
	 * @param jobName
	 * @param jobClass
	 * @param time
	 */
	public static void addJob(String jobName, String jobClass, String time) {
		try {
			Scheduler sch = factory.getScheduler();
			JobDetail detail = new JobDetail(jobName, JOB_GROUP_NAME, Class.forName(jobClass));
			CronTrigger trigger = new CronTrigger(jobName, TRIGGER_GROUP_NAME);
			trigger.setCronExpression(time);
			sch.scheduleJob(detail, trigger);
			if (!sch.isShutdown()) {
				sch.start();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void addJob(String jobName, String jobGrpName, String triggerName, String triggerGrpName,
			String jobCls, String time) {
		try {
			Scheduler sch = factory.getScheduler();
			JobDetail detail = new JobDetail(jobName, jobGrpName, Class.forName(jobCls));
			CronTrigger trigger = new CronTrigger(triggerName, triggerGrpName);
			trigger.setCronExpression(time);
			sch.scheduleJob(detail, trigger);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void modifyJobTime(String jobName, String time) {
		try {
			Scheduler sch = factory.getScheduler();
			CronTrigger trigger = (CronTrigger) sch.getTrigger(jobName, TRIGGER_GROUP_NAME);
			if (trigger == null) {
				return;
			}
			String oldTime = trigger.getCronExpression();
			if (!oldTime.equalsIgnoreCase(time)) {
				JobDetail detail = sch.getJobDetail(jobName, JOB_GROUP_NAME);
				Class jobCls = detail.getJobClass();
				String jobClass = jobCls.getName();
				removeJob(jobName);

				addJob(jobName, jobClass, time);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 修改一个任务的触发时间
	 * 
	 * @param triggerName
	 * @param triggerGroupName
	 * @param time
	 */
	public static void modifyJobTime(String triggerName, String triggerGroupName, String time) {
		try {
			Scheduler sched = factory.getScheduler();
			CronTrigger trigger = (CronTrigger) sched.getTrigger(triggerName, triggerGroupName);
			if (trigger == null) {
				return;
			}
			String oldTime = trigger.getCronExpression();
			if (!oldTime.equalsIgnoreCase(time)) {
				CronTrigger ct = (CronTrigger) trigger;
				// 修改时间
				ct.setCronExpression(time);
				// 重启触发器
				sched.resumeTrigger(triggerName, triggerGroupName);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 移除一个任务(使用默认的任务组名，触发器名，触发器组名)
	 * 
	 * @param jobName
	 */
	public static void removeJob(String jobName) {
		try {
			Scheduler sched = factory.getScheduler();
			sched.pauseTrigger(jobName, TRIGGER_GROUP_NAME);// 停止触发器
			sched.unscheduleJob(jobName, TRIGGER_GROUP_NAME);// 移除触发器
			sched.deleteJob(jobName, JOB_GROUP_NAME);// 删除任务
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 移除一个任务
	 * 
	 * @param jobName
	 * @param jobGroupName
	 * @param triggerName
	 * @param triggerGroupName
	 */
	public static void removeJob(String jobName, String jobGroupName, String triggerName, String triggerGroupName) {
		try {
			Scheduler sched = factory.getScheduler();
			sched.pauseTrigger(triggerName, triggerGroupName);// 停止触发器
			sched.unscheduleJob(triggerName, triggerGroupName);// 移除触发器
			sched.deleteJob(jobName, jobGroupName);// 删除任务
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 启动所有定时任务
	 */
	public static void startJobs() {
		try {
			Scheduler sched = factory.getScheduler();
			sched.start();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 关闭所有定时任务
	 */
	public static void shutdownJobs() {
		try {
			Scheduler sched = factory.getScheduler();
			if (!sched.isShutdown()) {
				sched.shutdown();
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
}
