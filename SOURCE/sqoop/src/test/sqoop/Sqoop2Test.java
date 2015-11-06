package test.sqoop;


import java.util.Date;

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

import bigdata.sqoop.tools.SqoopJobCallback;

public class Sqoop2Test {

	private static Logger log = Logger.getLogger(Sqoop2Test.class);
	
	public static void main(String[] args) {

		
		
		String url = "http://longan:12000/sqoop/";
		SqoopClient client = new SqoopClient(url);
		
		//client.setServerUrl(url);  另外一种方式
		
		
		
		MLink fromlink = client.createLink(4);  //通过client得到link对象
		
		fromlink.setName("mysql-con-" +System.currentTimeMillis());
		fromlink.setCreationDate(new Date());
		fromlink.setCreationUser("root");
		
		MLink toLink = client.createLink(2);
		toLink.setName("kite-con-"+System.currentTimeMillis());
		toLink.setCreationDate(new Date());
		toLink.setCreationUser("govnet");
		
		
	//	System.setProperty("HADOOP_USER_NAME", "root");
		
		//===from link
		MLinkConfig fromlinkConfig = fromlink.getConnectorLinkConfig();  //jdbc:mysql://192.168.1.8:3306/gather_mac
		fromlinkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:mysql://192.168.1.8:3306/gather_mac"); //设置link链接串
		fromlinkConfig.getStringInput("linkConfig.jdbcDriver").setValue("com.mysql.jdbc.Driver");  //设置驱动
		fromlinkConfig.getStringInput("linkConfig.username").setValue("mac"); 
		fromlinkConfig.getStringInput("linkConfig.password").setValue("mac123");
		
		//===to link
		MLinkConfig toLinkConfig = toLink.getConnectorLinkConfig();
		toLinkConfig.getStringInput("linkConfig.hdfsHostAndPort").setValue("longan:8020");
		//hdfs://govnet:8020 HDFS URL
		Status fromStatus = client.saveLink(fromlink);
		Status toStatus = client.saveLink(toLink);
		
		if(fromStatus.canProceed()&&toStatus.canProceed()){
			log.info("link saved success,with can used!");
		}else{
			log.info("link has not proceed!");
			System.out.println(fromStatus.name()+toStatus.name());
		}
		long fromLinkId = fromlink.getPersistenceId();
		long toLinkId  = toLink.getPersistenceId();
		
		System.out.println("f" +fromLinkId + "t"+toLinkId);
		
		//MJob job = client.getJob(4);
		MJob job = client.createJob(fromLinkId,toLinkId);  //开启一个job来执行
		
		String tabName = "LBS_TERMINAL_HIST";
		
		job.setName("mysql-to-hdsf"+tabName+System.currentTimeMillis());
		
		job.setCreationDate(new Date());
		
		job.setCreationUser("root");
		
		MFromConfig fromConfig = job.getFromJobConfig();
		
		
	  
/*String colNames = "ID,"+
				"CAST(DEV_LNG AS CHAR) DEV_LNG,"+
				"CAST(DEV_LAT AS CHAR) DEV_LAT,"+
				"DISTANCE,"+
				"DISTANCEINT,"+
				"CAST(USER_LNG AS CHAR) USER_LNG,"+
				"CAST(USER_LAT AS CHAR) USER_LAT,"+
				"PRAISERCOUNTALL,"+
				"ISSHIELD,"+
				"PRAISERCOUNTTODAY,"+
				"ISPRAISEDTODAY,"+
				"VISITORCOUNTALL,"+
				"USERID,"+
				"VISITORCOUNTTODAY ,"+
				"GROUPCOUNT ,"	+
				"ORIGINALURI ,"+
				"THUMBURI ,"+
				"SID ,"+
				"NICKNAME ,"+
				"SEX ,"+
				"CONSTELLATION ,"+
				"AGE ,"+
				"HOME ,"+
				"USERREGION ,"+
				"SCHOOL ,"+
				"STUDENT_CLASS ,"+
				"BIRTH_MONTH ,"+
				"BIRTH_DAY ,"+
				"MOOD ,"+
				"PORTRAITURI ,"+
				"URI ,"+
				"COLLECTIONMAC ,"+
				"DATE_FORMAT(COLLECTTIME,'%Y-%c-%d %h:%i:%s') COLLECTTIME ,"	+
				"DEV_ID ,"+
				"NODE_ID ,"+
				"DATA_SOURCE ,"+
				"DATE_FORMAT(INSERT_TIME,'%Y-%c-%d %h:%i:%s') INSERT_TIME ";*/
	   //fromConfig.getStringInput("fromJobConfig.schemaName").setValue("mysql");  //设置schema
		fromConfig.getStringInput("fromJobConfig.tableName").setValue(tabName);  //设置导入的表名

		//fromConfig.getStringInput("fromJobConfig.sql").setValue("SELECT ID,DATE_FORMAT(GATHER_TIME,'%Y-%c-%d %h:%i:%s') GATHER_TIME,MAC,DEV_MAC,SIGN,NODE_ID FROM LBS_TERMINAL_HIST WHERE ${CONDITIONS}");  //设置导入的sql脚本，若此项有数据则不要设置tableName的值，两者互斥
		fromConfig.getStringInput("fromJobConfig.partitionColumn").setValue("id");    //设置切分列名，一般使用主键
//		fromConfig.getStringInput("fromJobConfig.columns").setValue("id,DATE_FORMAT(COLLECTTIME,'%Y-%c-%d %h:%i:%s'),DATE_FORMAT(INSERT_TIME,'%Y-%c-%d %h:%i:%s'),DEV_LNG,DEV_LAT,DISTANCE,UID,DESCS,UID_OTHER,NAME,PROVINCE,CITY,COUNTRY,COLLECTIONMAC,NODE_ID,DATA_SOURCE,HEAD,HEAD2");
		//fromConfig.getStringInput("fromJobConfig.columns").setValue("id,name,sex,city");
	
		MToConfig toConfig = job.getToJobConfig();

		
		
		toConfig.getStringInput("toJobConfig.uri").setValue("dataset:"+"hdfs://govnet:8020/usr/tmp/kite/"+tabName);  //输出目录

		toConfig.getEnumInput("toJobConfig.fileFormat").setValue("AVRO");  //设置输出文件格式  CSV,AVRO,PARQUET
		//toConfig.getEnumInput("toJobConfig.compression").setValue("GZIP");     //设置压缩方式  NONE,DEFAULT,DEFLATE,GZIP,BZIP2,LZO,LZ4,SNAPPY,CUSTOM  CUSTOM需要设置toJobConfig.customCompression
		MDriverConfig driverConfig  = job.getDriverConfig();
		
		driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3); //设置导出文件分块数量   Loaders
	//	driverConfig.getInput("throttlingConfig.Loaders").setValue(2); //设置导出文件分块数量
		
		
		
		Status jobStatus = client.saveJob(job);   //保存job
		
		if(jobStatus.canProceed()){
			System.out.println("create job is :" + job.getPersistenceId());
		}else{
			System.out.println("Something wrong in create job");
		}
		
		//start job
		long jobId = job.getPersistenceId();
		System.out.println("job: " + jobId);
		//MSubmission submisson = client.startJob(jobId);
		MSubmission submisson;
		try {
			submisson = client.startJob(jobId, new SqoopJobCallback(), 5000);
			
			System.out.println("Job执行结束...");
			System.out.println("Hadoop任务ID为： " + submisson.getExternalJobId());
			Counters counters = submisson.getCounters();
			if(counters != null){
				System.out.println("计数器: ");
				for (CounterGroup counterGroup : counters) {
					System.out.println("\t");
					System.out.println(counterGroup.getName());
					for(Counter counter : counterGroup){
						System.out.println("\t\t");
						System.out.println(counter.getName());
						System.out.println(":  " + counter.getValue());
					}
				}
			}
			System.out.println("mysql to hdfs success!");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
