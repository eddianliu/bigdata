package bigdata.sqoop.tools;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
	
	//得到时间对应的毫秒数
	public static long getTimeWithStr(String source,String pattern) throws Exception{
		return new SimpleDateFormat(pattern).parse(source).getTime();
	}

	public static String getDateFromMills(String mills){
		Long timeMills = Long.valueOf(mills);
		Date d = new Date(timeMills);
		return new SimpleDateFormat("yyyyMMdd").format(d); 
	}
	
	public static Date parse(String DateStr) throws Exception{
		return new SimpleDateFormat("yyyyMMdd").parse(DateStr);
	}
	
	 public static String getDateAfter(String bDay, int day) throws Exception{ 
		    SimpleDateFormat fmt =  new SimpleDateFormat("yyyyMMdd");
	        Calendar now = Calendar.getInstance();  
	        now.setTime(fmt.parse(bDay));  
	        now.set(Calendar.DATE, now.get(Calendar.DATE) + day);  
	        return fmt.format(now.getTime());  
	    }
	
	public static void main(String[] args) throws Exception{
		System.out.println(getDateAfter("20150322", 72));
	}
}
