package bigdata.sqoop.tools;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import bigdata.sqoop.cfg.SqlDataItem;
import bigdata.sqoop.cfg.SqoopTaskInitiator;
import bigdata.sqoop.quartz.JobConfig;
import bigdata.sqoop.quartz.QuartzManager;
import bigdata.sqoop.tasks.SqoopImportThread;

public class SqoopImportTask {

	private static Logger log = Logger.getLogger(SqoopImportTask.class);
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("conf/log4j.properties");
		try{
			SqoopTaskInitiator initator = SqoopTaskInitiator.getInstance();
			if (!initator.init())
			{
				log.info(" --------------初始化sqoop数据文件参数出错----------");
				System.exit(0);
			}
			log.info(" --------------初始化sqoop数据文件参数完成----------");
			SqoopUtil util = SqoopUtil.getInstance();
			if (!util.init(4,1))
			{
				log.info(" --------------初始化sqoop参数出错----------");
				System.exit(0);
			}
			log.info(" --------------初始化sqoop参数完成----------");
			if(!JobConfig.getInstance().init()){
				log.info(" --------------初始化sqoop定时任务参数出错----------");
				System.exit(0);
			}
			log.info(" --------------初始化sqoop定时任务参数完成----------");
			
			Map<String,SqlDataItem> result = initator.getSqlItemMap();
			List<SqlDataItem> items = new ArrayList<SqlDataItem>();
			int i=0;
			if(result.keySet().size()>0){
					Set<String> set = result.keySet();
					for (String string : set) {
						SqlDataItem item = result.get(string);
						Date bDate = DateUtils.parse(item.getImpDate());
						while(bDate.getTime()<DateUtils.parse(JobConfig.getInstance().getQryEtime()).getTime()){
							SqlDataItem citem = item.clone();
							citem.setImpDate(DateUtils.getDateAfter(item.getImpDate(), i));
							citem.setId(citem.getId()+Integer.valueOf(i).toString());
							items.add(citem);
							bDate = DateUtils.parse(citem.getImpDate());
							i++;
						}
					}
					items.remove(items.size()-1); //去除最后一天的时间
					util.createSqpJobs(items);	
			}
			log.info("application has been finished");
			 //主线程等待
          /*  Object lock = new Object();
            synchronized (lock)
            {
                lock.wait();
            }*/
			//QuartzManager.addJob(SqoopConstants.SQP_IMP_JOBNAME, JobConfig.getInstance().getSchdCls().trim(),JobConfig.getInstance().getSchdCron().trim());
		}catch(Exception e){
			log.error("Error Exception:" + e.getMessage() , e);
		}
	}
	
}
