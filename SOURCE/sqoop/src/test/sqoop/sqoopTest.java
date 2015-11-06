package test.sqoop;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.tool.ImportTool;
import com.cloudera.sqoop.tool.SqoopTool;

public class sqoopTest {

	public static void Sqoop_Options() throws Exception {
		
		Configuration conf = new Configuration();
		SqoopOptions options = new SqoopOptions(conf);
		options.setAppendMode(true);
		options.setConnectString("jdbc:mysql://192.168.1.8:3306/gather_mac");
		options.setUsername("mac");
		options.setPassword("mac123");
		options.setTableName("LBS_APP_WEIXIN");// oracle表名
		options.setHiveTableName("lbs_app_weixin");//hive表名
		
		//Hive在hdfs上的位置：/test/user/hive/warehouse/表名/省编号分区/日期分区/
		String targetdir = "/usr/tmp/test/hive";

		options.setTargetDir(targetdir);
		//options.setWhereClause("PROVINCE='100' AND t_day like '2015%'");
		
		//存入hdfs后，字段间的分隔符
		options.setFieldsTerminatedBy('$');
		options.setHiveImport(true);
		options.setNumMappers(1);

		options.setExplicitInputDelims(true);
		/**
		 * exportTool：从hive导出到oracle
         * imports:从oracle导到hive
		 */
		SqoopTool imports = new ImportTool();
		int ret = imports.run(options);
		if (0 != ret) {
			throw new Exception("Error doing export; ret=" + ret);
		}
	}
	
	public static void main(String[] args) {
		try {
			Sqoop_Options();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
