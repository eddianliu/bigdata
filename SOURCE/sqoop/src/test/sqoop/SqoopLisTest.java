package test.sqoop;

import java.util.List;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;

public class SqoopLisTest {

	public static void main(String[] args) {

		String url = "http://t1m1:12000/sqoop/";
		SqoopClient client = new SqoopClient(url);
		
		List<MJob> jobs = client.getJobs();
		client.clearCache();
		for (MJob mJob : jobs) {
			client.deleteJob(mJob.getPersistenceId());
		}
		
		List<MLink> links = client.getLinks();
		
		
		for (MLink mLink : links) {
			client.deleteLink(mLink.getPersistenceId());
		}
		
		System.out.println("delete items success!");
		/*	for (MJob mJob : jobs) {
				System.out.println(mJob.getPersistenceId());
			}
			
			
			
			List<MLink> links = client.getLinks();
			for (MLink mLink : links) {
				System.out.println(mLink.getPersistenceId());
			}*/
	}

}
