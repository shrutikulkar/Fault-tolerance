package server.faulttolerance;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;
import client.MyDBClient;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.Queue;

/**
 * This class should implement your replicated fault-tolerant database server if
 * you wish to use Zookeeper or other custom consensus protocols to order client
 * requests.
 * <p>
 * Refer to {@link server.ReplicatedServer} for a starting point for how to do
 * server-server messaging or to {@link server.AVDBReplicatedServer} for a
 * non-fault-tolerant replicated server.
 * <p>
 * You can assume that a single fault-tolerant Zookeeper server at the default
 * host:port of localhost:2181 and you can use this service as you please in the
 * implementation of this class.
 * <p>
 * Make sure that both a single instance of Cassandra and a single Zookeeper
 * server are running on their default ports before testing.
 * <p>
 * You can not store in-memory information about request logs for more than
 * {@link #MAX_LOG_SIZE} requests.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer implements Watcher, StatCallback{
	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 */
	public static final int SLEEP = 1000;
	/**
	 * Set this to true if you want all tables drpped at the end of each run
	 * of tests by GraderFaultTolerance.
	 */
	public static final boolean DROP_TABLES_AFTER_TESTS=true;
	/**
	 * Maximum permitted size of any collection that is used to maintain
	 * request-specific state, i.e., you can not maintain state for more than
	 * MAX_LOG_SIZE requests (in memory or on disk). This constraint exists to
	 * ensure that your logs don't grow unbounded, which forces
	 * checkpointing to
	 * be implemented.
	 */
	public static final int MAX_LOG_SIZE = 400;
	public static final int DEFAULT_PORT = 2181;
	final private Session session;
    final private Cluster cluster;
	protected String leader;
	private String myID;
	private ZooKeeper zooKeeper ;
	private int NumAlivesrvr ;
	Watcher chainedWatcher;
    boolean dead;
    DataMonitorWatcher  listener;
    byte prevData[];
	protected final MessageNIOTransport<String,String> serverMessenger;
	int i=0;
	// this is the message queue used to track which messages have not been sent yet
    private ConcurrentHashMap<Long, JSONObject> queue = new ConcurrentHashMap<Long, JSONObject>();
    private CopyOnWriteArrayList<String> notAcked;
	private Queue<String> serverslist = new LinkedList<>();

	 // the sequencer to track the most recent request in the queue
	 private static long reqnum = 0;
	 synchronized static Long incrReqNum() {
		 return reqnum++;
	 }

	 // the sequencer to track the next request to be sent
	 private static long expected = 0;
	 synchronized static Long incrExpected() {
		 return expected++;
	 }

	/**
	 * @param nodeConfig Server name/address configuration information read
	 *                      from
	 *                   conf/servers.properties.
	 * @param myID       The name of the keyspace to connect to, also the name
	 *                   of the server itself. You can not connect to any other
	 *                   keyspace if using Zookeeper.
	 * @param isaDB      The socket address of the backend datastore to which
	 *                   you need to establish a session.
	 * @throws IOException
	 */
	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String
			myID, InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer
						.SERVER_PORT_OFFSET), isaDB, myID);
		log.log(Level.INFO, "Server {0} added cluster contact point",	new
		Object[]{myID,});
		session = (cluster=Cluster.builder().addContactPoint("127.0.0.1")
			.build()).connect(myID);
		
		
		this.myID = myID;
		this.leader = leader;
        this.chainedWatcher = chainedWatcher;
        this.listener = listener;
		this.zooKeeper  = new ZooKeeper("localhost:2181", 5000, null);
		this.serverMessenger =  new
                MessageNIOTransport<String, String>(myID, nodeConfig,
                new
                        AbstractBytePacketDemultiplexer() {
                            @Override
                            public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                                handleMessageFromServer(bytes, nioHeader);
                                return true;
                            }
                        }, true);

		// TODO: Make sure to do any needed crash recovery here.
		
		creatZnode(myID);
		setWatch();
		leader = getLeaderZnodes(myID);
		if(leader.equals(myID)){
			try {
				Stat stat = zooKeeper.exists("/queue", null);
				if (stat == null) {
					// Znode leader does not exist, create it
					String firstZnodeName = zooKeeper.create("/queue", null,
							ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				} else {
					// Znode already exists, retrieve and print its data
					byte[] data = zooKeeper.getData("/queue", false, null);
					//TODO: more to do. extract the query here and then add it to the hasmap queue
				}
				
			} catch (KeeperException | InterruptedException e) {
				creatZnode(myID);
			}
		}
		System.out.println("-----I got leader----"+leader);
		//NumAlivesrvr = getAlivesrvrcnt(myID);
		//System.out.println("--------number of alive servers:-----"+Integer.toString(NumAlivesrvr));
	
	}

	public interface DataMonitorWatcher {
		/**
		 * The existence status of the node has changed.
		 */
		void exists(byte data[]);
	
		/**
		 * The ZooKeeper session is no longer valid.
		 *
		 * @param rc
		 *                the ZooKeeper reason code
		 */
		void closing(int rc);
		
	}

	public void process(WatchedEvent event) {
        String path = event.getPath();
		//System.out.println("----------event.getpath() output--"+path);
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
            case SyncConnected:
                // In this particular example we don't need to do anything
                // here - watches are automatically re-registered with
                // server and any watches triggered while the client was
                // disconnected will be delivered (in order of course)
                break;
            case Expired:
                // It's all over
                dead = true;
                listener.closing(KeeperException.Code.SessionExpired);
                break;
            }
        } else {
			if (path != null && path.equals("/leader")) {
				// Something has changed on the node, let's find out
				System.out.println("-------we enter this condition-----");
				//zooKeeper.exists("/leader", true, this, null);
				if (event.getType() == Event.EventType.NodeDataChanged) {
					// Znode data changed, handle the event
					System.out.println("/leader znode changed. new Leader is");
					getLeaderZnodes(myID);
				} else if (event.getType() == Event.EventType.NodeDeleted) {
					// Znode deleted, print a message and call getLeaderZnodes again
					System.out.println("/leader znode deleted. Electing new Leader.");
					getLeaderZnodes(myID);
				} else {
					System.out.println("The event being triggered here is : " + event.getType().toString());
				}
			}
			if (path != null && path.equals("/server0")) {
				if (event.getType() == Event.EventType.NodeCreated) {
					NumAlivesrvr++;
				} 
				else if (event.getType() == Event.EventType.NodeDeleted) {
					System.out.println("----server 0 deleted----");
					NumAlivesrvr--;
				} else {
					System.out.println("The event being triggered here is : " + event.getType().toString());
				}
				
			}

			if (path != null && path.equals("/server1")) {
				if (event.getType() == Event.EventType.NodeCreated) {
					NumAlivesrvr++;
				} 
				else if (event.getType() == Event.EventType.NodeDeleted) {
					System.out.println("----server 1 deleted----");
					NumAlivesrvr--;
				} else {
					System.out.println("The event being triggered here is : " + event.getType().toString());
				}
				
			}

			if (path != null && path.equals("/server2")) {
				if (event.getType() == Event.EventType.NodeCreated) {
					NumAlivesrvr++;
				} 
				else if (event.getType() == Event.EventType.NodeDeleted) {
					System.out.println("----server 2 deleted----");
					NumAlivesrvr--;
				} else {
					System.out.println("The event being triggered here is : " + event.getType().toString());
				}
				
			}
			
        }
        if (chainedWatcher != null) {
            chainedWatcher.process(event);
        }
    }

	public void processResult(int rc, String path, Object ctx, Stat stat) {
        boolean exists;
        switch (rc) {
        case Code.Ok:
            exists = true;
            break;
        case Code.NoNode:
            exists = false;
            break;
        case Code.SessionExpired:
        case Code.NoAuth:
            dead = true;
            listener.closing(rc);
            return;
        default:
            // Retry errors
            zooKeeper.exists(leader, true, this, null);
            return;
        }

        byte b[] = null;
        if (exists) {
            try {
                b = zooKeeper.getData(leader, false, null);
            } catch (KeeperException e) {
                // We don't need to worry about recovering now. The watch
                // callbacks will kick off any exception handling
                e.printStackTrace();
            } catch (InterruptedException e) {
                return;
            }
        }
        if ((b == null && b != prevData)
                || (b != null && !Arrays.equals(prevData, b))) {
            listener.exists(b);
            prevData = b;
        }
    }

	/**
	 * TODO: process bytes received from clients here.
	 */
	protected static enum Type {
		REQUEST, // a server forwards a REQUEST to the leader
		PROPOSAL, // the leader broadcast the REQUEST to all the nodes
        ACKNOWLEDGEMENT; // all the nodes send back acknowledgement to the leader
	}
	
	@Override
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		// this is a request sent by callbackSend method
		String request = new String(bytes);
		log.log(Level.INFO, "{0} received client message {1} from {2}",
                new Object[]{this.myID, request, header.sndr});
        JSONObject json = null;
        try {
            json = new JSONObject(request);
            request = json.getString(MyDBClient.Keys.REQUEST
                    .toString());
        } catch (JSONException e) {
            //e.printStackTrace();
        }
		
		// forward the request to the leader as a proposal        
		try {
			JSONObject packet = new JSONObject();
			packet.put(MyDBClient.Keys.REQUEST.toString(), request);
			packet.put(MyDBClient.Keys.TYPE.toString(), Type.REQUEST.toString());			
			
			this.serverMessenger.send(leader, packet.toString().getBytes());
			log.log(Level.INFO, "{0} sends a REQUEST {1} to {2}", 
					new Object[]{this.myID, packet, leader});
		} catch (IOException | JSONException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
        
        String response = "[success:"+new String(bytes)+"]";       
        if(json!=null){
        	try{
        		json.put(MyDBClient.Keys.RESPONSE.toString(),
                        response);
                response = json.toString();
            } catch (JSONException e) {
                e.printStackTrace();
        	}
        }
        
        try{
	        // when it's done send back response to client
	        serverMessenger.send(header.sndr, response.getBytes());
        } catch (IOException e) {
        	e.printStackTrace();
        }
	}

	/**
	 * TODO: process bytes received from fellow servers here.
	 */
	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) { 
        // deserialize the request
        JSONObject json = null;
		try {
			json = new JSONObject(new String(bytes));
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        log.log(Level.INFO, "{0} received relayed message {1} from {2}",
                new Object[]{this.myID, json, header.sndr}); // simply log
        
        // check the type of the request
        try {
			String type = json.getString(MyDBClient.Keys.TYPE.toString());
			if (type.equals(Type.REQUEST.toString())){
				//System.out.println("------got request ----"+Type.REQUEST.toString());
				if(myID.equals(leader)){
					// put the request into the queue
					Long reqId = incrReqNum();
					json.put(MyDBClient.Keys.REQNUM.toString(), reqId);
					queue.put(reqId, json);

					//set /queue znode with the queue that we have got

					log.log(Level.INFO, "{0} put request {1} into the queue.",
			                new Object[]{this.myID, json});
					// backup the queue to a persistant znode. 
					if(isReadyToSend(expected)){
			        	// retrieve the first request in the queue
						JSONObject proposal = queue.remove(expected);
						if(proposal != null) {
							proposal.put(MyDBClient.Keys.TYPE.toString(), Type.PROPOSAL.toString());
							enqueue();
							broadcastRequest(proposal);
						} else {
							log.log(Level.INFO, "{0} is ready to send request {1}, but the message has already been retrieved.",
					                new Object[]{this.myID, expected});
						}
						
			        }
				} else {
					log.log(Level.SEVERE, "{0} received REQUEST message from {1} which should not be here.",
			                new Object[]{this.myID, header.sndr});
				}
			} else if (type.equals(Type.PROPOSAL.toString())) {
				
				// execute the query and send back the acknowledgement
				String query = json.getString(MyDBClient.Keys.REQUEST.toString());
				long reqId = json.getLong(MyDBClient.Keys.REQNUM.toString());
				// Logging the query
				logQuery(query);
				session.execute(query);
				System.out.println("----I executed this query---"+query);
				JSONObject response = new JSONObject().put(MyDBClient.Keys.RESPONSE.toString(), this.myID)
						.put(MyDBClient.Keys.REQNUM.toString(), reqId)
						.put(MyDBClient.Keys.TYPE.toString(), Type.ACKNOWLEDGEMENT.toString());
				serverMessenger.send(header.sndr, response.toString().getBytes());
			} else if (type.equals(Type.ACKNOWLEDGEMENT.toString())) {
				
				// only the leader needs to handle acknowledgement
				if(myID.equals(leader)){
					// TODO: leader processes ack here
					String node = json.getString(MyDBClient.Keys.RESPONSE.toString());
					if (dequeue(node)){
						// if the leader has received all acks, then prepare to send the next request
						expected++;
						if(isReadyToSend(expected)){
							System.out.println("----I will now send next request---");
							JSONObject proposal = queue.remove(expected);
							if(proposal != null) {
								proposal.put(MyDBClient.Keys.TYPE.toString(), Type.PROPOSAL.toString());
								enqueue();
								broadcastRequest(proposal);
							} else {
								log.log(Level.INFO, "{0} is ready to send request {1}, but the message has already been retrieved.",
						                new Object[]{this.myID, expected});
							}
						}
					}
				} else {
					log.log(Level.SEVERE, "{0} received ACKNOWLEDEMENT message from {1} which should not be here.",
			                new Object[]{this.myID, header.sndr});
				}
			} else {
				log.log(Level.SEVERE, "{0} received unrecongonized message from {1} which should not be here.",
		                new Object[]{this.myID, header.sndr});
			}
			
		} catch (JSONException | IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    }

	private void creatZnode(String servername){
		Stat stat;
		try {
			// Check if the znode already exists
			zooKeeper.addWatch("/"+servername, event -> process(event), AddWatchMode.PERSISTENT);
			stat = zooKeeper.exists("/"+servername, null);
			if (stat == null) {
				// Znode leader does not exist, create it
				String firstZnodeName = zooKeeper.create("/"+servername, servername.getBytes(),
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				System.out.println("Znode created: " + firstZnodeName);
			} else {
				// Znode already exists, retrieve and print its data
				byte[] data = zooKeeper.getData("/"+servername, false, null);
				String znodeData = new String(data);
				System.out.println("Znode already exists. Current znode is: " + znodeData);
			}
			
		} catch (KeeperException | InterruptedException e) {
			creatZnode(myID);
		}
	}

	private void setWatch(){
		try {
			zooKeeper.addWatch("/server0", event -> process(event), AddWatchMode.PERSISTENT);
			zooKeeper.addWatch("/server1", event -> process(event), AddWatchMode.PERSISTENT);
			zooKeeper.addWatch("/server2", event -> process(event), AddWatchMode.PERSISTENT);
		} catch (KeeperException | InterruptedException e) {
			setWatch();
		}
	}
	private String getLeaderZnodes(String servername) {
		Stat stat = null;
		try {
			// Check if the znode already exists
			zooKeeper.addWatch("/leader", event -> process(event), AddWatchMode.PERSISTENT);
			stat = zooKeeper.exists("/leader", null);
			if (stat == null) {
				// Znode leader does not exist, create it
				String firstZnodeName = zooKeeper.create("/leader", servername.getBytes(),
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				System.out.println("Leader Znode created: " + firstZnodeName);
				return servername;
			} else {
				// Znode already exists, retrieve and print its data
				byte[] data = zooKeeper.getData("/leader", false, null);
				String znodeData = new String(data);
				System.out.println("Leader already exists. Current Leader is: " + znodeData);
				return znodeData;
			}
			
		} catch (KeeperException | InterruptedException e) {
			String leader_test;
			e.printStackTrace();
			leader_test = getLeaderZnodes(myID);
			//System.out.println("Why am I here");
			return leader_test; 
		}
	}
	
	private Integer getAlivesrvrcnt(String servername) {
		try {
			// Check if the znode already exists
			Stat stat = zooKeeper.exists("/totsrvr", event -> process(event));
			if (stat == null) {
				//this is the first server to get added.
				zooKeeper.create("/totsrvr", servername.getBytes(),
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				zooKeeper.create("/totsrvr/"+myID, null, 
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				//serverslist.add(servername);
				return 1;
			} else {
				Stat stat_2 = zooKeeper.exists("/totsrvr/"+myID, null);
				if (stat_2 == null){
					zooKeeper.create("/totsrvr/"+myID, null, 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				}
				int cntChildren = zooKeeper.getAllChildrenNumber("/totsrvr");
				//serverslist.add(servername);
				return cntChildren;
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			int total_servers = getAlivesrvrcnt(myID);
			return total_servers; 
		}
	}
	
	private void logQuery(String query) {
		String logFilePath = "src/server/faulttolerance/Post_CP_query_LOGs.log";

		try {
			File logFile = new File(logFilePath);

			// Check if the log file exists, if not, create it
			if (!logFile.exists()) {
				logFile.createNewFile();
			}

			// Check the number of lines in the log file
			int currentLines = countLines(logFile);

			if (currentLines >= MAX_LOG_SIZE) {
				// Call checkpoint function and clear the file if it returns true
				if (checkpoint()) {
					clearLogFile(logFile);
				}
			}

			try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile, true))) {
				// If the file is not empty, add a new line before writing the query
				if (logFile.length() > 0) {
					writer.newLine();
				}

				writer.write(query);
			}
		} catch (IOException e) {
			e.printStackTrace(); 
		}
	}

	private boolean checkpoint() {
		// Implement your checkpoint logic here
		// If checkpoint is successful, return true; otherwise, return false
		return true;
	}

	private void clearLogFile(File logFile) {
		try (PrintWriter writer = new PrintWriter(logFile)) {
			// Clear the file by opening it in write mode
		} catch (FileNotFoundException e) {
			e.printStackTrace(); // Handle the exception based on your application's needs
		}
	}

	private int countLines(File file) throws IOException {
		try (LineNumberReader reader = new LineNumberReader(new FileReader(file))) {
			while (reader.skip(Long.MAX_VALUE) > 0) {
				// Skip to the end of the file
			}
			return reader.getLineNumber();
		}
	}
	
	private boolean isReadyToSend(long expectedId) {
		System.out.println("------i am in ready to send-----"+queue.size()+queue.containsKey(expectedId));
		if (queue.size() > 0 && queue.containsKey(expectedId)) {
			return true;
		}
		return false;
	}
	
	private void broadcastRequest(JSONObject req) {
		System.out.println("----i will broadcast to --- "+serverslist);
		for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()){
            try {
                this.serverMessenger.send(node, req.toString().getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
		}
		log.log(Level.INFO, "The leader has broadcast the request {0}", new Object[]{req});
	}
	
	private void enqueue(){
		notAcked = new CopyOnWriteArrayList<String>();
		for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()){
            notAcked.add(node);
		}
	}
	
	private boolean dequeue(String node) {
		if(!notAcked.remove(node)){
			log.log(Level.SEVERE, "The leader does not have the key {0} in its notAcked", new Object[]{node});
		}
		int downsrvrcount = 0;
		System.out.println("-----7&&&&&&&&&&&&&&&&&&----"+NumAlivesrvr);
		downsrvrcount = 3 - NumAlivesrvr;
		for(int i=0; i<downsrvrcount; i++){
			notAcked.remove(node);
		}
		System.out.println("--------downsrvrcnt------"+downsrvrcount);
		if(notAcked.size() == 0){
			return true;
		}
		return false;
	}

	/**
	 * TODO: Gracefully close any threads or messengers you created.
	 */
	@Override
	public void close() {
	    super.close();
	    this.serverMessenger.stop();
	    session.close();
	    cluster.close();
	}

	public static enum CheckpointRecovery {
		CHECKPOINT, RESTORE;
	}

	/**
	 * @param args args[0] must be server.properties file and args[1] must be
	 *             myID. The server prefix in the properties file must be
	 *             ReplicatedServer.SERVER_PREFIX. Optional args[2] if
	 *             specified
	 *             will be a socket address for the backend datastore.
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile

				(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
						.SERVER_PORT_OFFSET), args[1], args.length > 2 ? Util
				.getInetSocketAddressFromString(args[2]) : new
				InetSocketAddress("localhost", 9042));
	}


}