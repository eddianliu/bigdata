package bigdata.sqoop.cfg;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;


public class SqlDataItem implements Cloneable{

	private String id;
	
	private String partition;
	
	private String columns;
	
	private String tableName;
	
	private String condition;

	private String impDate;
	
	private int numExt;
	
	
	
	public int getNumExt() {
		return numExt;
	}

	public void setNumExt(int numExt) {
		this.numExt = numExt;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPartition() {
		return partition;
	}

	public void setPartition(String partition) {
		this.partition = partition;
	}

	public String getColumns() {
		return columns;
	}

	public void setColumns(String columns) {
		this.columns = columns;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getCondition() {
		return condition;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}
	
	public String getImpDate() {
		return impDate;
	}

	public void setImpDate(String impDate) {
		this.impDate = impDate;
	}

	public SqlDataItem(String id,String partition,int numExt,String columns,String tableName,String condition,String impDate){
		this.id = id;
		this.partition = partition;
		this.numExt = numExt;
		this.tableName = tableName;
		this.columns = columns;
		this.condition = condition;
		this.impDate = impDate;
	}
	
	public SqlDataItem clone(){
		SqlDataItem ite = null;
		try {
			ite = (SqlDataItem)super.clone();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ite;
	}
	
	@Override
	public String toString(){
		Map<String, Object> mmp = new HashMap<String,Object>();
		mmp.put("id", this.id);
		mmp.put("partition", this.partition);
		mmp.put("numExt", this.numExt);
		mmp.put("tableName", this.tableName);
		mmp.put("columns", this.columns);
		mmp.put("condition", this.condition);
		mmp.put("impDate", this.impDate);
		return JSONObject.toJSONString(mmp);
	}
}
