package bigdata.sqoop.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

import bigdata.hdfs.HDFSUtils;
import bigdata.sqoop.cfg.SqlDataItem;
import bigdata.sqoop.quartz.JobConfig;


public class SqoopUtil {

	private static Logger log = Logger.getLogger(SqoopUtil.class);
	
	private Properties prop = new Properties();
	
	private long flnkId;
	
	private long tlnkId;
	
	private static SqoopUtil util = new SqoopUtil();
	
	public static SqoopUtil getInstance(){
		return util;
	}
	
	public boolean init(int source,int target){
		String path = System.getProperty("user.dir")+File.separator+SqoopConstants.DIC_CONF+File.separator+SqoopConstants.SQOOP_PROP;
		boolean flag = false;
		try {
			InputStream is = new FileInputStream(path);
			prop.load(is);
			SqoopClient client = getClient();
			this.flnkId= createLink(client,source);
			this.tlnkId = createLink(client,target);
			
			flag = true;
		}catch(SQLException se){
			log.info("properties JDBC配置出错!");
		}catch (FileNotFoundException fe) {
			log.info("conf文件夹下缺失sqoop配置文件");
		}catch(Exception e){
			e.printStackTrace();
		}
		return flag;
	}
	
	public SqoopClient getClient() throws Exception{
		String uri = prop.getProperty(SqoopConstants.CLIENT_PATH);
		if(StringUtils.isEmpty(uri)){
			throw new Exception("sqoop客户端链接未配置");
		}
		SqoopClient client = new SqoopClient(uri);
		return client;
	}
	
	private String getLinkName(long type){
		if(type == ConEnum.KAFKA.value()){
			 return prop.getProperty("kafka-lnk.name");
		}else if(type == ConEnum.KITE.value()){
			 return prop.getProperty("kite-lnk.name");
		}else if(type == ConEnum.HDFS.value()){
			 return prop.getProperty("hdfs-lnk.name");
		}else if(type == ConEnum.JDBC.value()){
			 return prop.getProperty("jdbc-lnk.name");
		}else{
			return "UNKOWN";
		}
	}
	
	private long isExsitsLnkName(SqoopClient client,String lnkName){
		List<MLink> lnks = client.getLinks();
		long flag = -1;
		for (MLink mLink : lnks) {
			if(lnkName.equals(mLink.getName())){
				flag = mLink.getPersistenceId();
				break;
			}
		}
		return flag;
	}
	
	private long createLink(SqoopClient client, long type) throws Exception{
		long lnkId = isExsitsLnkName(client, getLinkName(type));
		MLink link;
		if(lnkId>-1){
			 link = client.getLink(lnkId);
		}else{
			 link = client.createLink(type);
		}
		link.setName(getLinkName(type));
		link.setCreationDate(new Date());
		link.setCreationUser(SqoopConstants.LINK_USER);
		setLnkConfig(link, type);
		Status status;
		if(lnkId>-1){
			 status = client.updateLink(link);
		}else{
			 status = client.saveLink(link);
		}
		if(status.canProceed()){
			log.info("link saved success,with can used!");
		}else{
			log.info("link has not proceed withe the status: " + status.name());
			throw new Exception("from链接创建失败，失败状态为" + status.name());
		}
		long linkId = link.getPersistenceId();
		return linkId;
	}
	
	/**
	 * 拼接完整sql
	 * @param columns
	 * @param table
	 * @param where
	 * @return
	 */
	public String genSqlState(String impDate,String columns,String table,String where)throws Exception{
		if(StringUtils.isEmpty(columns)||StringUtils.isEmpty(table))
			return null;
		StringBuilder builder = new StringBuilder("SELECT ");
		builder.append(columns).append(" FROM ").append(table);
		if(!StringUtils.isEmpty(where)){
			/*String wheresql = "INSERT_TIME>='"+transDate(where, 0)+" 00:00:00'"+ " AND INSERT_TIME<'"+transDate(where, 86400000)+" 00:00:00'";
					builder.append(" WHERE ").append(wheresql).append(" and ${CONDITIONS}");*/
			builder.append(" WHERE ").append(genWhereSql(impDate,where)).append(" and ${CONDITIONS}");
		}else
			builder.append(" WHERE ${CONDITIONS}");
		log.info("SQL: " + builder.toString());
		return builder.toString();
	}
	
	public static String genWhereSql(String impDate,String where) throws Exception{
		return where.replaceAll(":CBTIME", "'"+impDate+" 00:00:00'").replaceAll(":CETIME", "'"+DateUtils.getDateAfter(impDate, 1)+" 00:00:00'");
	}
	
	private long createJob(SqoopClient client,long flnkId,long tlnkId,SqlDataItem item)throws Exception{
		MJob job = client.createJob(flnkId, tlnkId);
		job.setName("Sqoop-" + item.getTableName() + "-" + System.currentTimeMillis());
		job.setCreationDate(new Date());
		job.setCreationUser(SqoopConstants.LINK_USER);
		
		MFromConfig fConfig = job.getFromJobConfig();
		setFromConfigWithJDBC(fConfig,item);
		
		MToConfig tConfig = job.getToJobConfig();
		setToConfigWithHDSF(tConfig,item);
		
		MDriverConfig dConfig = job.getDriverConfig();
		setDriverConfig(dConfig,item);
		Status jobStatus = client.saveJob(job);
		if(jobStatus.canProceed()){
			System.out.println("create job is :" + job.getPersistenceId());
		}else{
			System.out.println("Something wrong in create job");
		}
		return job.getPersistenceId();
	}
	
	
	private void setFromConfigWithJDBC(MFromConfig fConfig,SqlDataItem item)throws Exception{
		fConfig.getStringInput(SqoopConstants.FROM_JDBC_SQL).setValue(genSqlState(item.getImpDate(),item.getColumns(), item.getTableName(), item.getCondition()));
		log.info("JOB-PARTITION: " + item.getPartition());
		fConfig.getStringInput(SqoopConstants.FROM_JDBC_PARTI).setValue(item.getPartition());
	//	fConfig.getStringInput(SqoopConstants.FROM_JDBC_COLUMNS).setValue(item.getColumns());
	}
	
	private Integer getParti(SqlDataItem item) throws Exception{
		 int res = 0;
		 Class.forName(prop.getProperty(SqoopConstants.JDBC_DRIVER));
		 Connection con = DriverManager.getConnection(prop.getProperty(SqoopConstants.JDBC_CONSTR),
				 									  prop.getProperty(SqoopConstants.JDBC_UNAME),
				 									  prop.getProperty(SqoopConstants.JDBC_PWD));
		 Statement st = con.createStatement();
		 String sql = genWhereSql(item.getImpDate(), prop.getProperty(SqoopConstants.CAPACITY_SQL));
		 log.info("count-sql: " +sql);
		 ResultSet rs = st.executeQuery(sql);
		 while(rs.next()){
			 String num = rs.getString("num");
			 int capi = (1024*1024)/200;
			 res = Integer.valueOf(num)/capi; //MB
		 }
		 int blockSize = Integer.valueOf(prop.get(SqoopConstants.HDFS_BLOCK_SIZE).toString());
		 int result = res/blockSize;
		 log.info("MR分片：" + result); 
		return result;
	}

	private void setToConfigWithHDSF(MToConfig tConfig,SqlDataItem item){
		tConfig.getStringInput(SqoopConstants.TO_URI).setValue("dataset:"+prop.getProperty(SqoopConstants.TO_URI)+item.getImpDate()+"/"+item.getTableName());
		tConfig.getEnumInput(SqoopConstants.TO_FORMAT).setValue(prop.getProperty(SqoopConstants.TO_FORMAT));
		
	}
	
	private void setDriverConfig(MDriverConfig driverConfig,SqlDataItem item){
		//int ret = numExtByTableName(item.getTableName());
		try {
			driverConfig.getIntegerInput(SqoopConstants.TO_EXT).setValue(getParti(item)==0?1:getParti(item));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	private void setLnkConfig(MLink mlik, long type){
		MLinkConfig config = mlik.getConnectorLinkConfig();
		if(type == ConEnum.KAFKA.value()){
			
		}else if(type == ConEnum.KITE.value()){
			config.getStringInput(SqoopConstants.HDFS_HOST_WITH_PORT).setValue(prop.getProperty(SqoopConstants.HDFS_HOST_WITH_PORT));;
		}else if(type == ConEnum.HDFS.value()){
			config.getStringInput(SqoopConstants.HDFS_URI).setValue(prop.getProperty(SqoopConstants.HDFS_URI));
		}else if(type == ConEnum.JDBC.value()){
			config.getStringInput(SqoopConstants.JDBC_CONSTR).setValue(prop.getProperty(SqoopConstants.JDBC_CONSTR));
			config.getStringInput(SqoopConstants.JDBC_DRIVER).setValue(prop.getProperty(SqoopConstants.JDBC_DRIVER));
			config.getStringInput(SqoopConstants.JDBC_UNAME).setValue(prop.getProperty(SqoopConstants.JDBC_UNAME));
			config.getStringInput(SqoopConstants.JDBC_PWD).setValue(prop.getProperty(SqoopConstants.JDBC_PWD));
		}else{
			config = null;
		}
	}
	
	/**
	 * 开始执行导入功能
	 * @throws Exception
	 */
	public void startSqoopJobs() throws Exception{
		SqoopClient client = getClient();
		List<MJob> jobs = client.getJobs();
		List<MJob> runJobs = new ArrayList<MJob>();
		if(jobs.size()>0){
			int i=0;
			for (MJob mJob : jobs) {
				if(client.getJobStatus(mJob.getPersistenceId()).getStatus().name().equals(SqoopConstants.SQP_JOB_NOWORK)){ //job未执行过
					String hdfsDir = mJob.getToJobConfig().getStringInput(SqoopConstants.TO_URI).getValue();
					log.info(hdfsDir);
					if(!HDFSUtils.isExsistFileType(hdfsDir.substring(hdfsDir.indexOf("hdfs"), hdfsDir.length()), "avro")){ //job对应的hdfs目录下没有生成avro文件
						runJobs.add(mJob);
						i++;
						if(i==20){   //集合满20个则退出循环
							break;
						}
					}
				}
			}
		}
		log.info("runJob: " +runJobs.size());
		if(runJobs.size()>0){  //每次开启20个JOB
			for (MJob mJob : runJobs) {
				log.info("submission is beginning");
				MSubmission submisson = client.startJob(mJob.getPersistenceId(), new SqoopJobCallback(), Integer.valueOf(prop.getProperty(SqoopConstants.LOG_SEPREATOR)));
				log.info("Job"+submisson.getExternalJobId()+"excuted successful ...");
			}
		}
	}
	
	/**
	 * 创建JOB
	 * @param items
	 * @return
	 * @throws Exception
	 */
	public List<Long> createSqpJobs(List<SqlDataItem> items) throws Exception{
		SqoopClient client = getClient();
		List<Long> result = new ArrayList<Long>();
		for (SqlDataItem item : items) {
			log.info("create job ..."+ item.toString());
			long jobId = createJob(client, flnkId, tlnkId,item);
			log.info(String.format("start sqoop job , jobId : %d", jobId));
			MSubmission	submisson = client.startJob(jobId, new SqoopJobCallback(), Integer.valueOf(prop.getProperty(SqoopConstants.LOG_SEPREATOR)));
			log.info(String.format("end sqoop job , jobId : %d", jobId));
			log.info("Hadoop任务ID为： " + submisson.getExternalJobId());
			
			log.info("db to hdfs success!");
			result.add(jobId);
		}
		return result;
	}
	
	/**
	 * @author Jun.L
	 * @param source 源数据库链接编号
	 * @param target 目标连接编号
	 * @return true or false
	 * @throws Exception
	 */
	public boolean SqoopImport(SqlDataItem item) throws Exception{
		SqoopClient client = getClient();
		
		long jobId = createJob(client, flnkId, tlnkId,item);
		if(jobId ==-1){
			throw new Exception("创建job失败，请检查job创建参数");
		}
		MSubmission submisson;
		try {
			log.info(String.format("start sqoop job , jobId : %d", jobId));
			submisson = client.startJob(jobId, new SqoopJobCallback(), Integer.valueOf(prop.getProperty(SqoopConstants.LOG_SEPREATOR)));
			log.info(String.format("end sqoop job , jobId : %d", jobId));
			log.info("Hadoop任务ID为： " + submisson.getExternalJobId());
			
			log.info("db to hdfs success!");
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public static void main(String[] args) {
		int res = Integer.valueOf(498401)*200/(1024*1024);
		System.out.println(res/128);
	}
}
