package server.faulttolerance;

import com.datastax.driver.core.*;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
		import java.lang.reflect.Field;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*; 


/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 * Faster
	 * is not necessarily better, so don't sweat speed. Focus on safety.
	 */
	public static final int SLEEP = 1000;
	public String keyspace;
    Cluster cluster;
	Session session;
	int countRequest;
	Session session1;
	String myID;
	int counter =0;

	List<Request> req = new ArrayList<Request>();

	//protected int total = 0;


	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {
		
		// TODO: setup connection to the data store and keyspace


		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect(args[0]);
		keyspace = args[0];
		myID = args[0];
		countRequest = 0;



		 

		//throw new RuntimeException("Not yet implemented");


	}

	/**
	 * Refer documentation of {@link Replicable#execute(Request, boolean)} to
	 * understand what the boolean flag means.
	 * <p>
	 * You can assume that all requests will be of type {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
	 *
	 * @param request
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean b) {
		// TODO: submit request to data store

		/*countRequest++;
		if ( countRequest == 400)
		checkpoint(keyspace);*/
	//	req.add(request);

		//System.out.println("%%%%%%%%%%%% Execute first %%%%%%%%%%%%%%%");
	//	String temp = checkpoint(keyspace);
		//System.out.println(temp);
		return this.execute(request);

		
	}

	private String getRandomAliveServer(String node) {
		//keep getting a node until the node value is not equal to node
		// once got the different node return that node

	//	while()
       return "server1";
    }

	private boolean retryRequest(Request request, String node) {
        try {
            // Get a random alive server
            String aliveServer = getRandomAliveServer(node);

            // Retry the request on the chosen server
            return execute(request, false);
        } catch (Exception e) {
            // Handle exception, e.g., log the error
            System.err.println("Error retrying request: " + e.getMessage());
            return false;
        }
    }


	
	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
	//System.out.println("%%%%%%%%%%%%%%%  Execute second  %%%%%%%%%%%%%%%%%%%");

try {
	req.add(request);

	counter++;
	/*if(counter == 20)
	{
	checkpoint(keyspace);
	}*/


		if (request instanceof RequestPacket) {
			String requestValue = ((RequestPacket) request).requestValue;
			/*((RequestPacket) request).setResponse("echoing [" +
					requestValue + "]");*/

			ResultSet resultSet = session.execute(requestValue);

			return true;
			
		}
		else 
		{
			System.err.println("Unknown packet type: " + request.getSummary());
			return false;
		}

	

	}
	catch (Exception e) {
		// Handle exception, e.g., log the error
		System.err.println("Error executing request: " + e.getMessage());
		return false;

	//	return retryRequest(request,myID);

	}

	}

	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String s) {

				


			StringBuilder checkpointString = new StringBuilder("");
				String originalTable = keyspace + ".grade";
			String checkpointTableName = s + ".grade_checkpoint";
			ArrayList<String> results = new ArrayList<String>();
	
		System.out.println("(((((((((((((((((((((name)))))))))))))))))))))    "+keyspace);

		try {


				System.out.println("I am here ****************");
				//return "hello_____________________________________";

							
			ResultSet resultSet = session.execute("SELECT * FROM " + originalTable);
			//System.out.println("++++++++++++++++++sit++++++++++++++++"+resultSet.toString());
            for (Row row : resultSet) {
				String id = row.getString("id");
				String events = row.getString("events");
				String query = "insert into  grade (id, events) values (" + id + "," + events + ");";
                results.add(query);
			}
				//results.add("hello");

			for (Request ind_req : req) {
				System.out.println("Individual request " + ind_req.toString());
				//results.add("hello");
				results.add(ind_req.toString());
			}
			req.clear();
			



				//System.out.println("checkpointString.toString()checkpointString.toString()checkpointString.toString()checkpointString.toString() "+checkpointString.toString());
			


			System.out.println("************************* ROW EXTRACTED *******************************");

			System.out.println(checkpointString.toString());
			String final_result = "";
			for (String ind_result : results) {
				final_result = final_result + ind_result;

			}
			System.out.println("The final query being sent in checkpoint is " + final_result);
				return final_result;

		}

		catch (Exception e) {
			//throw new RuntimeException("Failed to create checkpoint table", e);
			for (Request ind_req : req) {
				System.out.println("Individual request " + ind_req.toString());
				//results.add("hello");
				results.add(ind_req.toString());
			}
			req.clear();
			System.out.println(checkpointString.toString());
			String final_result = "";
			for (String ind_result : results) {
				final_result = final_result + ind_result;

			}
			System.out.println("The final query being sent in checkpoint is " + final_result);
				return final_result;
		}

	}

			// Retrieve the metadata of the original table
/* 
			session.execute("drop table if exists " +  checkpointTableName);

			StringBuilder createTableStatement = new StringBuilder("CREATE TABLE " + checkpointTableName + " ( id int PRIMARY KEY,     events list<int>);");

				System.out.println(createTableStatement.toString());

				ResultSet sourceResultSet = session.execute("SELECT * FROM " + originalTable);

			
				}

				String insertStatement = "insert into " + checkpointTableName + " select * from " + originalTable;
				session.execute(insertStatement.toString());

				System.out.println("!!!!!!!!!!!!! all checkpoint commands successfully executed!!!!!!!");

			
		} 
		catch (Exception e) {
			throw new RuntimeException("Failed to create checkpoint table", e);
		}
  	//throw new RuntimeException("Not yet implemented");
	return checkpointTableName;
	*/

	
/**
	 * Refer documentation of {@link Replicable#restore(String, String)}
	 *
	 * @param s
	 * @param s1
	 * @return
	 */
	@Override
	public boolean restore(String s, String s1) {

		System.out.println("getting s1 in restore as : " + s1);
		if(s1.equals(new String("{}"))) {
			return true;
		}

		ArrayList<String> myList = new ArrayList<String>(Arrays.asList(s1.split(";")));

		// System.out.println("myID"+myID);
		// String truncate_query = "truncate " + keyspace + ".grade;";
		// session.execute(truncate_query);

		for (String query: myList) {
			String new_query = query + ";";
			System.out.println("Final query to be executed in restore is " + new_query);
			session.execute(new_query);
		}
		// TODO:
		//throw new RuntimeException("Not yet implemented");

      //  session.execute("CREATE KEYSPACE IF NOT EXISTS " + s + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}");
		//check if the key space is empty
		// if the key space is empty.... then server was down from beginning. so ask for the cp from other server/RSM
		//ask the rsm to get the recent cp.

		//System.out.println("extracted "+s1);

		/*if(keyspace==null)
		{

		}

		String[] rows = s1.split("\n");
		//session.execute("truncate table " + myID + ".grade");
			System.out.print("myIDmyIDmyIDmyIDmyIDmyIDmyIDmyIDmyIDmyID" +keyspace);


		for (String row : rows) {
			System.out.print(row);
			if (!row.isEmpty()) {
				//session.execute("INSERT INTO "  + keyspace + ".grade "  + row);
				System.out.println("check restore");
			}
						System.out.println();

		}
*/


		return true;

	}



	/**
	 * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
	 * .RequestPacket will be used by Grader, so you don't need to implement
	 * this method.}
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 * example of how to define your own IntegerPacketType enum, refer {@link
	 * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
	 * need to be implemented because the assignment Grader will only use
	 * {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}
}
