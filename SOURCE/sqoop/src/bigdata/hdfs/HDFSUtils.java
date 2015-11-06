package bigdata.hdfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * HDFS操作类
 * @author Liu.J
 *
 */
public class HDFSUtils {

	private static Logger log = Logger.getLogger(HDFSUtils.class);
	
	 //在指定位置新建一个文件，并写入字符  
    public static void WriteToHDFS(String file, String words) throws IOException, URISyntaxException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(file), conf);  
        Path path = new Path(file);  
        FSDataOutputStream out = fs.create(path);   //创建文件  

        
        //两个方法都用于文件写入，好像一般多使用后者  
        out.writeBytes(words);    
        out.write(words.getBytes("GBK"));  
          
        out.close();  
        //如果是要从输入流中写入，或是从一个文件写到另一个文件（此时用输入流打开已有内容的文件）  
        //可以使用如下IOUtils.copyBytes方法。  
        //FSDataInputStream in = fs.open(new Path(args[0]));  
        //IOUtils.copyBytes(in, out, 4096, true)        //4096为一次复制块大小，true表示复制完成后关闭流  
    }  
      
    public static void ReadFromHDFS(String file) throws IOException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(file), conf);  
        Path path = new Path(file);  
        FSDataInputStream in = fs.open(path);  
          
        IOUtils.copyBytes(in, System.out, 4096, true);  
        //使用FSDataInoutStream的read方法会将文件内容读取到字节流中并返回  
        /** 
         * FileStatus stat = fs.getFileStatus(path); 
      // create the buffer 
       byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))]; 
       is.readFully(0, buffer); 
       is.close(); 
             fs.close(); 
       return buffer; 
         */  
    }  
      
    public static void DeleteHDFSFile(String file) throws IOException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(file), conf);  
        
        Path path = new Path(file);  
        //查看fs的delete API可以看到三个方法。deleteonExit实在退出JVM时删除，下面的方法是在指定为目录是递归删除  
        fs.delete(path,true);   //true递归删除对应路径，false则表示删除当前文件而已
        fs.close();  
    }  
      
    public static void UploadLocalFileHDFS(String src, String dst) throws IOException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(dst), conf);  
        Path pathDst = new Path(dst);  
        Path pathSrc = new Path(src);  
          
        fs.copyFromLocalFile(pathSrc, pathDst);  
        fs.close();  
    }  
      
    public static void ListDirAll(String DirFile) throws IOException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(DirFile), conf);  
        Path path = new Path(DirFile);  
          
        FileStatus[] status = fs.listStatus(path);  
        //方法1    
        for(FileStatus f: status)  
        {  
            log.info(Long.valueOf(f.getLen()).toString());
            log.info(f.getPath().toString());
            log.info(f.getGroup());
            log.info(Long.valueOf(f.getBlockSize()).toString());
        }  
        //方法2    
        Path[] listedPaths = FileUtil.stat2Paths(status);    
        for (Path p : listedPaths){   
          log.info(p.toString());  
        }  
    }  
	
    /**
     * 判断HDFS目录是否存在文件类型
     * @param dirFile
     * @param fileType
     * @return
     * @throws IOException
     */
    public static boolean isExsistFileType(String dirFile,String fileType) throws IOException{
    	 Configuration conf = new Configuration();  
         FileSystem fs = FileSystem.get(URI.create(dirFile), conf);  
         Path path = new Path(dirFile); 
         if(!fs.exists(path)){return false;} //如果dir目录都不存在，直接返回false
         boolean flag = false;  
         FileStatus[] status = fs.listStatus(path);  
         //方法1    
         for(FileStatus f: status)  
         {  
            if(f.getPath().getName().contains(fileType)){
            	flag = true;
            	break;
            }
         }  
      return flag;
    }
    
    /**
     * 判断HDFS文件是否存在
     * @param path
     * @return
     * @throws Exception
     */
    public static boolean isExsitsHDFSFile(String path)throws Exception{
    	 Configuration conf = new Configuration();  
         FileSystem fs = FileSystem.get(URI.create(path), conf); 
         Path hdfsPath = new Path(path);  
         return fs.exists(hdfsPath);
    }
    
    /**
     * 判断本地文件是否存在
     * @param filePath
     * @return
     */
    public static boolean isExsitsLocalFile(String filePath){
    	return new File(filePath).exists();
    }
    
    public static void main(String[] args) {
		try {
			System.out.println(isExsitsHDFSFile("hdfs://114.215.188.78:8020/usr/data_imp/usr.avro"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
