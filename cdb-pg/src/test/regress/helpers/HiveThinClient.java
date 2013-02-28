import java.util.List;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

/*
 * Simple Hive client that can replace the Hive cli. It accepts any hiveQL command and sends it
 * to the Hive Thrift server. This class is needed since we cannot run a Hive cli on the same machine
 * where there is a Hive Thrift server running. Since Hive Thrift server is a basic component for our
 * Hive support solution, we had to come up with something that can replace the Hive cli and this is
 * where HiveThinClient comes in.
 */
public class HiveThinClient 
{ 		   
	public static void main(String[] args) throws Exception 
	{ 
		if (args.length < 1)
		{
			System.out.println("usage: java HiveThinClient  \"<hiveQL command>\" ");
			return;
		}
		
		
		TSocket transport = new TSocket("localhost" , 10000);
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		HiveClient client = new org.apache.hadoop.hive.service.HiveClient(protocol);
		transport.open();
		
		// sending the command to the Hive thrift server
		client.execute(args[0]);
		
		List<String> resp = client.fetchAll();
		int sz = resp.size();
		
		System.out.println("number of lines: " + sz);
		for (int i = 0; i < sz; i++)
			System.out.println(resp.get(i));
		
		transport.close();
	}
}








