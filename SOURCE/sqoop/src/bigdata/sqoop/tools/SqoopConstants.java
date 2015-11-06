package bigdata.sqoop.tools;

public interface SqoopConstants {

	final static String CLIENT_PATH = "client.uri";  //SQOOP客户端链接URI
	final static String FROM_CONNECT = "flink.conId";  //flink的连接ID
	final static String TO_CONNECT = "tlink.conId"; //tlink的连接ID
	final static String LINK_USER = "lnk.usr";  //lnk创建人
	
	final static String LINK_TYPE = "lnk.type";  //lnk类型
	final static String LINK_ID = "lnk.Id";  
	final static String LOG_SEPREATOR = "log.separetor"; //job日志写入时间间隔
	
	final static String CAPACITY_SQL ="capacity.sql"; //计算job数量sql
	
	final static String  HDFS_BLOCK_SIZE= "hdfs.block.size";  //hdfs文件块大小
	//Kite lnk conf
	final static String HDFS_HOST_WITH_PORT = "linkConfig.hdfsHostAndPort"; //hdfs主机和端口
	
	//jdbc lnk conf
	final static String JDBC_DRIVER = "linkConfig.jdbcDriver";   //jdbc驱动
	final static String JDBC_CONSTR = "linkConfig.connectionString"; //jdbc链接字符串
	final static String JDBC_UNAME = "linkConfig.username"; //jdbc用户名
	final static String JDBC_PWD = "linkConfig.password";   //jdbc密码
	
	//hdfs lnk conf
	final static String HDFS_URI = "linkConfig.hdfsuri";   //hdfs访问uri
	
	//job from conf
	final static String FROM_JDBC_SCHEMA = "fromJobConfig.schemaName";
	final static String FROM_JDBC_TABNAME = "fromJobConfig.tableName";
	final static String FROM_JDBC_SQL = "fromJobConfig.sql";
	final static String FROM_JDBC_COLUMNS = "fromJobConfig.columns";
	final static String FROM_JDBC_PARTI = "fromJobConfig.partitionColumn";

	//job to conf
	final static String TO_URI="toJobConfig.uri";
	final static String TO_FORMAT="toJobConfig.fileFormat";
	final static String TO_EXT="throttlingConfig.numExtractors";
	
	final static String DIC_CONF = "conf";
	final static String SQOOP_PROP = "sqoop.properties";
	final static String DATA_XML = "sql-data.xml";
	final static String LOG_FILE = "log4j.properties";
	
	final static String SQP_JOB_NOWORK = "NEVER_EXECUTED";  //sqoop-job初始状态
	final static String SQP_IMP_JOBNAME = "sqoopImpfromOra2hdfs";  //sqoop定时任务名称
	
	final static String DATE_PATTERN = "yyyyMMdd";
	final static int DAY_TILLS = 86400000;
}
