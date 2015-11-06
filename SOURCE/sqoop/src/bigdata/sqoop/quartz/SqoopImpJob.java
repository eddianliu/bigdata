package bigdata.sqoop.quartz;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import bigdata.sqoop.tools.SqoopUtil;

public class SqoopImpJob implements Job {

	private Logger log = Logger.getLogger(this.getClass());
	
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		log.info("sqoop-import task is excuteding");
		SqoopUtil util = SqoopUtil.getInstance();
		try {
			util.startSqoopJobs();
			
		} catch (Exception e) {
			log.info(e);
		}
		
	}

}
