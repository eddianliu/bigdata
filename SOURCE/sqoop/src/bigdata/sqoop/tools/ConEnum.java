package bigdata.sqoop.tools;

public enum ConEnum {

	KITE(1),KAFKA(2),HDFS(3),JDBC(4);
	
	private int value = 0;
	
	private ConEnum(int value){
		this.value = value;
	}
	
	public static ConEnum valueOf(int value){
		switch(value){
		case 1:
			return KITE;
		case 2:
			return KAFKA;
		case 3:
			return HDFS;
		case 4:
			return JDBC;
		default:
				return null;
		}
	}
	
	public int value(){
		return this.value;
	}
}
