package test.sqoop;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;


public class MysqlMetaTest {

	public static void main(String[] args) {
		try {
		 Class.forName("oracle.jdbc.driver.OracleDriver");
		 String uri = "jdbc:oracle:thin:@192.168.1.2:1521:orclutf8";
		 String uname = "i_other";String pwd = "i_other";
		 Connection con = DriverManager.getConnection(uri,uname,pwd);
		 String sql = "insert into userdetail(id,name,addr) values(?,?,?)";
		 PreparedStatement ps = con.prepareCall(sql);
		
			 for(int j=0;j<10000;j++){
				 ps.setInt(1, j);
				 ps.setString(2, UUID.randomUUID().toString());
				 ps.setString(3, UUID.randomUUID().toString());
				 ps.addBatch();
			 }
			 ps.executeBatch();
		 
	} catch (Exception e) {
		e.printStackTrace();
	}
	}

}
