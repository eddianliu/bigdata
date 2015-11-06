package test.sqoop;

import java.util.List;
import java.util.ResourceBundle;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.SqoopProtocolConstants;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MSubmission;

public class SqoopConfigTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String url = "http://115.29.249.170:12000/sqoop/";
		SqoopClient client = new SqoopClient(url);
		List<MJob> lis = client.getJobs();
		for (MJob mJob : lis) {
			System.out.println(mJob.getPersistenceId()+"status: "+client.getJobStatus(mJob.getPersistenceId()).getStatus().name());
		}
		/*long connectorId =1;
		System.out.println(client.getLinks().get(0).getName());

		
		
		// link config for connector
		describe(client.getConnector(connectorId).getLinkConfig().getConfigs(), client.getConnectorConfigBundle(connectorId));
		// from job config for connector
	  describe(client.getConnector(connectorId).getFromConfig().getConfigs(), client.getConnectorConfigBundle(connectorId));
		// to job config for the connector
	  describe(client.getConnector(connectorId).getToConfig().getConfigs(), client.getConnectorConfigBundle(connectorId));*/
	

}

	
	
	public static void describe(List<MConfig> configs, ResourceBundle resource) {
		  for (MConfig config : configs) {
		    System.out.println(resource.getString(config.getLabelKey())+":");
		    List<MInput<?>> inputs = config.getInputs();
		    for (MInput input : inputs) {
		      System.out.println(resource.getString(input.getLabelKey()) + " : " + input.getValue());
		    }
		    System.out.println();
		  }
		}
}
