package bigdata.sqoop.cfg;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import bigdata.sqoop.tools.SqoopConstants;


public class SqoopTaskInitiator {

	private static SqoopTaskInitiator initator = new SqoopTaskInitiator();
	
	private  Map<String,SqlDataItem> result = new HashMap<String, SqlDataItem>();
	
	private  String SELECT = "select";
	
	private SqoopTaskInitiator(){
		
	}
	
	public static SqoopTaskInitiator getInstance(){
		return initator;
	}
	
	public boolean init(){
		String path = System.getProperty("user.dir")+File.separator+SqoopConstants.DIC_CONF+File.separator+SqoopConstants.DATA_XML;
		InputStream is; boolean flag = false;
		try {
			is = new FileInputStream(path);
			analyzeXML(is);
			flag = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}
	
	/**
	 * 解析xml数据配置
	 * @param is
	 * @throws Exception
	 */
	public void analyzeXML(InputStream is)throws Exception{
		SAXReader reader= new SAXReader();
		
		Document doc = reader.read(is);
		Element root = doc.getRootElement();
		List<Element> selItems = root.elements(SELECT);
		if(selItems.size()>0){
			for (Element element : selItems) {
				parseSqlItems(element);
			}
		}else{
			throw new Exception("沒有配置查询节点信息(no select)");
		}
	}

	private void parseSqlItems(Element element) {
		String id = element.attributeValue("id");
		String partition = element.attributeValue("partition");
		String numExt =   element.attributeValue("numExt");
		String columns = element.elementText("columns");
		String tableName = element.elementText("table");
		String condition = element.elementText("where");
		String impDate = element.getParent().attributeValue("date");
		SqlDataItem item = new SqlDataItem(id, partition, Integer.valueOf(numExt), columns, tableName, condition,impDate);
		result.put(id, item);
	}
	
	public Map<String,SqlDataItem> getSqlItemMap(){
		return result;
	}
	
}
