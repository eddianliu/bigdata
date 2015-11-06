package test.sqoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class HadoopTest {

	private static final String HADOOP_URL = "hdfs://govnet:8020";
	
	public static void main(String[] args) {
		try {
			//System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master");
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
			final URL url = new URL(HADOOP_URL);
			final InputStream in = url.openStream();
			OutputStream out = new FileOutputStream(new File("d://h1.txt"));
			IOUtils.copyBytes(in, out, 1024,true);
			out.close();
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}	
		
		
	}

}
