package bigdata.sqoop.tasks;

import java.util.List;

import bigdata.sqoop.cfg.SqlDataItem;
import bigdata.sqoop.tools.SqoopUtil;

public class SqoopImportThread  extends Thread{

	private List<SqlDataItem> items;
	private SqoopUtil util;
	
	public SqoopImportThread(List<SqlDataItem> items,SqoopUtil util){
		this.items = items;
		this.util = util;
	}
	
	@Override
	public void run() {
		try {
			util.createSqpJobs(items);
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}

}
