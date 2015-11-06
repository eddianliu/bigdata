package bigdata.sqoop.quartz;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import bigdata.sqoop.tools.SqoopConstants;

public class JobConfig {

	private String schdCls;   //定时任务实现类全路径
	private String schdCron;     //定时任务执行表达式
	private String qryEtime; //任务查询截至日期 
	private static JobConfig config = new JobConfig();
	
	public static JobConfig getInstance(){
		return config;
	}
	
	public boolean init(){
		try{
			Properties prop= new Properties();
			String url = System.getProperty("user.dir")+File.separator+"conf"+File.separator+"sqoop.properties";
			InputStream sis = new FileInputStream(url);
			prop.load(sis);
			
			schdCls = prop.getProperty("schedule.cls");
			schdCron = prop.getProperty("schedule.cron");
			qryEtime = prop.getProperty("query.eTime");
			return true;
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
	}

	public String getQryEtime() {
		return qryEtime;
	}

	public void setQryEtime(String qryEtime) {
		this.qryEtime = qryEtime;
	}

	public String getSchdCls() {
		return schdCls;
	}

	public void setSchdCls(String schdCls) {
		this.schdCls = schdCls;
	}

	public String getSchdCron() {
		return schdCron;
	}

	public void setSchdCron(String schdCron) {
		this.schdCron = schdCron;
	} 
	
	
}
