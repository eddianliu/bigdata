package test.sqoop;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import bigdata.sqoop.cfg.SqlDataItem;
import bigdata.sqoop.quartz.JobConfig;
import bigdata.sqoop.tools.DateUtils;


public class SqoopTools {
	
	public static void  main(String[] args){
		List<SqlDataItem> items = new ArrayList<SqlDataItem>();
		int i=0;
		try {
			SqlDataItem item = new SqlDataItem("id"+i, "1123", i, "tt111", "test", "conditeion", "20150801");
			Date bDate = DateUtils.parse(item.getImpDate());
			while(bDate.getTime()<DateUtils.getTimeWithStr("20150803", "yyyyMMdd")){
				SqlDataItem citem = item.clone();
				citem.setImpDate(DateUtils.getDateAfter(item.getImpDate(), i));
				citem.setId(citem.getId()+Integer.valueOf(i).toString());
				items.add(citem);
				bDate = DateUtils.parse(citem.getImpDate());
				i++;
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		items.remove(items.size()-1);
		for (SqlDataItem sqlDataItem : items) {
			System.out.println(sqlDataItem.toString());
		}		
				
	}
}
