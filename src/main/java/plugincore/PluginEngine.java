package plugincore;

import ActiveMQ.*;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import netdiscovery.DiscoveryClientIPv6;
import netdiscovery.DiscoveryEngine;
import netdiscovery.DiscoveryType;
//import shared.Clogger;
import shared.MsgEvent;
import shared.MsgEventType;
import shared.PluginImplementation;
import shared.RandomString;

import java.io.File;
import java.io.FileInputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

public class PluginEngine {
    
	
	public static String pluginName;
	public static String pluginVersion;
	public static String plugin;
	public static String agent;
	public static String region;
	public static String agentpath;
	
	public static HashMap<String,Long> rpcMap;
	public static CommandExec commandExec;
	public static PluginConfig config;
	public static ConcurrentLinkedQueue<MsgEvent> msgInQueue;
	
	
	public static boolean clientDiscoveryActive = false;
	public static boolean clientDiscoveryActiveIPv6 = false;
	public static boolean DiscoveryActive = false;
	public static boolean ActiveBrokerManagerActive = false;
	public static boolean ActiveDestManagerActive = false;
	public static boolean ConsumerThreadActive = false;
	public static boolean ConsumerThreadRegionActive = false;
	
	public static boolean restartOnShutdown = false;

	public static Thread discoveryEngineThread;
	public static Thread activeBrokerManagerThread;
	public static Thread consumerRegionThread;
	public static Thread consumerAgentThread;
	public static Thread shutdownHook;
	public static WatchDog watchDogProcess;

	public static ActiveProducer ap;
	
	public static String brokerAddress;
	public static boolean isRegionalController = false;
	public static boolean isIPv6 = false;
	public static boolean isActive = false;
	
	public static int responds = 0;
	
	
	public static ConcurrentHashMap<String,BrokeredAgent> brokeredAgents;
	
	public static ConcurrentLinkedQueue<MsgEvent> incomingCanidateBrokers;
	public static ConcurrentLinkedQueue<MsgEvent> outgoingMessages;
	
	//public static DiscoveryClientIPv4 dc;
	public static DiscoveryClientIPv6 dcv6;

	//public static Clogger logger;
	private static final Logger logger = LoggerFactory.getLogger(PluginEngine.class);
	
	public PluginEngine()
	{
		try
		{
			rpcMap = new HashMap<String,Long>();
			pluginName = getPluginName();
			pluginVersion = getPluginVersion();
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			System.out.println("PluginEngine: Could not set plugin name: " + ex.toString());
			pluginName="cresco-agent-controller-plugin";
			pluginVersion="unknown";
		}
		
	}
	
	public String getName()
	{
		   return pluginName; 
	}
	public String getVersion() //This should pull the version information from jar Meta data
    {
			return pluginVersion;
	}
	public void msgIn(MsgEvent me)
	{
		
		final MsgEvent ce = me;
		try
		{
		Thread thread = new Thread(){
		    public void run(){
		
		    	try 
		        {
		    		MsgEvent re = commandExec.cmdExec(ce);
		    		if(re != null)
					{
						re.setReturn(); //reverse to-from for return
						msgInQueue.offer(re); //send message back to queue
					}
					
				} 
		        catch(Exception ex)
		        {
		        	logger.error("msgIn Thread run: " + ex.getMessage());
		        }
		    }
		  };
		  thread.start();
		}
		catch(Exception ex)
		{
			logger.error("msgIn Thread: " + ex.getMessage());        	
		}
		
	}
	
	public boolean initialize(ConcurrentLinkedQueue<MsgEvent> msgOutQueue, ConcurrentLinkedQueue<MsgEvent> msgInQueue, SubnodeConfiguration configObj, String region, String agent, String plugin) {
		//logger = new Clogger(msgOutQueue, region, agent, plugin);
		commandExec = new CommandExec();
		
		try
		{
			PluginEngine.agent = agent;
			PluginEngine.plugin = plugin;
			PluginEngine.region = region;
			PluginEngine.config = new PluginConfig(configObj);
			PluginEngine.msgInQueue = msgInQueue; //messages to agent should go here
		
		}
		catch(Exception ex)
		{
			logger.error("Could not create plugin object: " + ex.getMessage());
		}
		
		return true;
	}
	
	public static ActiveBroker broker;
/*
    public static void main(String[] args) throws Exception
    {
    	
    	if(args.length == 1)
    	{
    		Thread.sleep(Integer.parseInt(args[0])*1000);
    	}
		shutdownHook = new Thread(new Runnable() {
			public void run() {
				try {
					if (isActive)
						shutdown();
				} catch(Exception ex) {
					logger.error("Run {}", ex.getMessage());
				}
			}
		}, "Shutdown-thread");
		Runtime.getRuntime().addShutdownHook(shutdownHook);

		
    	RandomString rs = new RandomString(4);
		agent = "agent-" + rs.nextString();
		logger.debug("Generating Agent identity = [" + agent + "]");
    	
		
        commInit(); //initial init

		logger.info("Agent [{}] running...", agentpath);

		System.out.print("Name of Agent to message [q to quit]: ");
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		String input = scanner.nextLine();

		while (!input.toLowerCase().equals("q")) {
			if (input.length() == 0) {
				List<String> rAgents = reachableAgents();
				if (rAgents.isEmpty()) {
					logger.info("No agents found... " + responds);
				} else {
					logger.info("Sending message to {} agents", rAgents.size());
					for(String str : rAgents) {
						logger.info("\t" + str);
					}
					logger.info("Found {} agents", rAgents.size());
				}
			} else {
				if(input.toLowerCase().equals("all")) {
					List<String> rAgents = reachableAgents();
					if(rAgents.isEmpty()) {
						logger.info("No agents found...");
					} else {
						logger.info("Sending message to {} agents", rAgents.size());
						for(String agent : rAgents) {
							logger.info("\t" + agent);
							sendMessage(MsgEventType.INFO, agent, "ping");
						}
						logger.info("Found {} agents", rAgents.size());
					}
				} else {
					sendMessage(MsgEventType.INFO, input, "ping");
				}
			}
			System.out.print("Name of Agent to message [q to quit]: ");
			input = scanner.nextLine();
		}
		shutdown();
    }
*/
    public static void commInit()
    {
		logger.info("Initializing services");
    	PluginEngine.isActive = true;
    	try
        {
        	brokeredAgents = new ConcurrentHashMap<>();
        	incomingCanidateBrokers = new ConcurrentLinkedQueue<>();
        	outgoingMessages = new ConcurrentLinkedQueue<>();
        	brokerAddress = null;
        	isIPv6 = isIPv6();
        	

        	dcv6 = new DiscoveryClientIPv6();
            //dc = new DiscoveryClientIPv4();

        	logger.debug("Broker Search...");
    		List<MsgEvent> discoveryList = dcv6.getDiscoveryResponse(DiscoveryType.AGENT,2000);
			logger.info("discoverylist count= " + discoveryList.size());
			//logger.info("Broker search IPv4:");
    		//dc.getDiscoveryMap(2000);
    		if(discoveryList.isEmpty())
    		{
    			//generate regional ident if not assigned
    			//String oldRegion = region; //keep old region if assigned
    			
    			if((region == "init") && (agent == "init"))
    			{
    				RandomString rs = new RandomString(4);
        			region = "region-" + rs.nextString();
        			agent = "agent-" + rs.nextString();
        			//logger.warn("Agent region changed from :" + oldRegion + " to " + region);
    			}
    			logger.debug("Generated regionid=" + region);
    			agentpath = region + "_" + agent;
    			logger.debug("AgentPath=" + agentpath);
    			//Start controller services
    			
    			//discovery engine
    			discoveryEngineThread = new Thread(new DiscoveryEngine());
    			discoveryEngineThread.start();
    	    	while(!DiscoveryActive)
    	        {
    	        	Thread.sleep(1000);
    	        }
    	        //logger.debug("IPv6 DiscoveryEngine Started..");
    			
    	        logger.info("Broker started");
    	        broker = new ActiveBroker(agentpath);
    	        
    	        
    	        //broker manager
    	        activeBrokerManagerThread = new Thread(new ActiveBrokerManager());
    	        activeBrokerManagerThread.start();
				/*synchronized (activeBrokerManagerThread) {
					activeBrokerManagerThread.wait();
				}*/
    	    	while(!ActiveBrokerManagerActive)
    	        {
    	        	Thread.sleep(1000);
    	        }
    	        //logger.debug("ActiveBrokerManager Started..");
    	        
    	        if(isIPv6) { //set broker address for consumers and producers
    	    		brokerAddress = "[::1]";
    	    	} else {
    	    		brokerAddress = "localhost";
    	    	}
    	        
    	        //consumer region 
    	        consumerRegionThread = new Thread(new ActiveRegionConsumer(region, "tcp://" + brokerAddress + ":32010"));
    	        consumerRegionThread.start();
    	    	while(!ConsumerThreadRegionActive)
    	        {
    	        	Thread.sleep(1000);
    	        }
    	        //logger.debug("Region ConsumerThread Started..");
        		
    	        isRegionalController = true;
    	        //start regional discovery
    	        discoveryList.clear();
    	        discoveryList = dcv6.getDiscoveryResponse(DiscoveryType.REGION,2000);
    	        if(!discoveryList.isEmpty())
        		{
    	        	for(MsgEvent ime : discoveryList)
    	        	{
    	        		incomingCanidateBrokers.offer(ime);
    	        		logger.debug("Region Found: " + ime.getParams());
        	        	
    	        	}
        		}
    	        
    		} else {
    			//determine least loaded broker
    			//need to use additional metrics to determine best fit broker
    			String cbrokerAddress = null;
    			String cRegion = null;
    			int brokerCount = -1;
    			for (MsgEvent bm : discoveryList) {
    				
    				int tmpBrokerCount = Integer.parseInt(bm.getParam("agent_count"));
    				if(brokerCount < tmpBrokerCount) {
    					logger.debug("commInit {}", bm.getParams().toString());
						cbrokerAddress = bm.getParam("dst_ip");
    					cRegion = bm.getParam("src_region");
    				}
    			}
    			if (cbrokerAddress != null) {
    				InetAddress remoteAddress = InetAddress.getByName(cbrokerAddress);
    				if(remoteAddress instanceof Inet6Address) {
    					cbrokerAddress = "[" + cbrokerAddress + "]";
    				}
    				brokerAddress = cbrokerAddress;
    				region = cRegion;
    				logger.debug("Assigned regionid=" + region);
        			agentpath = region + "_" + agent;
    				logger.debug("AgentPath=" + agentpath);
        			
    				
    			}
    			isRegionalController = false;
    		}
    		
    		//consumer agent 
	        consumerAgentThread = new Thread(new ActiveAgentConsumer(agentpath,"tcp://" + brokerAddress + ":32010"));
	        consumerAgentThread.start();
	    	while(!ConsumerThreadActive)
	        {
	        	Thread.sleep(1000);
	        }
	        //logger.debug("Agent ConsumerThread Started..");
    		
	        ap = new ActiveProducer("tcp://" + brokerAddress + ":32010");
	        logger.info("Producer started");

	        watchDogProcess = new WatchDog();
			logger.info("WatchDog started");
    	} catch(Exception e) {
    		e.printStackTrace();
    		logger.error("commInit " + e.getMessage());
    	}
      
    }

    public static void shutdown() {
		try {
			logger.info("Shutting down");
			if(watchDogProcess != null) {
				watchDogProcess.shutdown();
				watchDogProcess = null;
			}

			DiscoveryActive = false;
			if(discoveryEngineThread != null) {
				logger.trace("Discovery Engine shutting down");
				DiscoveryEngine.shutdown();
				discoveryEngineThread.join();
				discoveryEngineThread = null;
				isActive = false;

			}
			ConsumerThreadRegionActive = false;
			if(consumerRegionThread != null) {
				logger.trace("Region Consumer shutting down");
				consumerRegionThread.join();
				consumerRegionThread = null;
			}

			ConsumerThreadActive = false;
			if(consumerAgentThread != null) {
				logger.trace("Agent Consumer shutting down");
				consumerAgentThread.join();
				consumerAgentThread = null;
			}

			ActiveBrokerManagerActive = false;
			if(activeBrokerManagerThread != null) {
				logger.trace("Active Broker Manager shutting down");
				activeBrokerManagerThread.join();
				activeBrokerManagerThread = null;
			}
			if (ap != null) {
				logger.trace("Producer shutting down");
				ap.shutdown();
				ap = null;
			}
			if(broker != null) {
				logger.trace("Broker shutting down");
				broker.stopBroker();
				broker = null;

			}
			if(restartOnShutdown) {
				commInit(); //reinit everything
				restartOnShutdown = false;
			}
		} catch(Exception ex) {
			logger.error("shutdown {}", ex.getMessage());
		}
		
	}
	
    public static boolean sendMessage(MsgEventType type, String targetAgent, String msg) {
		if (isReachableAgent(targetAgent)) {
			logger.debug("Sending message to Agent [{}]", targetAgent);
			String[] str = targetAgent.split("_");
			MsgEvent sme = new MsgEvent(type, region, agent, plugin, msg);
			sme.setParam("src_region", region);
			sme.setParam("src_agent", agent);
			sme.setParam("dst_region", str[0]);
			if(str.length == 2) {
				sme.setParam("dst_agent", str[1]); //send to region if agent does not exist
			}
			ap.sendMessage(sme);
			return true;
		} else {
			logger.error("Attempted to send message to unreachable agent [{}]", targetAgent);
			return false;
		}
	}
    
    public static boolean isLocal(String checkAddress) {
    	boolean isLocal = false;
    	if(checkAddress.contains("%")) {
    		String[] checkScope = checkAddress.split("%");
    		checkAddress = checkScope[0];
    	}
    	List<String> localAddressList = localAddresses();
    	for(String localAddress : localAddressList) {
    		if(localAddress.contains(checkAddress)) {
    			isLocal = true;
    		}
    	}
    	return isLocal;
    }

    public static List<String> localAddresses() {
    	List<String> localAddressList = new ArrayList<>();
    	try {
			Enumeration<NetworkInterface> inter = NetworkInterface.getNetworkInterfaces();
			while (inter.hasMoreElements()) {
				NetworkInterface networkInter = inter.nextElement();
				for (InterfaceAddress interfaceAddress : networkInter.getInterfaceAddresses()) {
					String localAddress = interfaceAddress.getAddress().getHostAddress();
					if(localAddress.contains("%")) {
						String[] localScope = localAddress.split("%");
						localAddress = localScope[0];
					}
					if(!localAddressList.contains(localAddress)) {
						localAddressList.add(localAddress);
					}
				}
			}
    	} catch(Exception ex) {
			logger.error("localAddresses Error: {}", ex.getMessage());
    	}
    	return localAddressList;
    }
    
    public static boolean isIPv6() {
    	boolean isIPv6 = false;
    	try {
    		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
  	 	  	while (interfaces.hasMoreElements()) {
  	 	  		NetworkInterface networkInterface = interfaces.nextElement();
  	 	  		if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.isUp() || !networkInterface.supportsMulticast() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
  	 	  			continue; // Don't want to broadcast to the loopback interface
  	 	  		}
  	 	  		if(networkInterface.supportsMulticast()) {
  	 	  			for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses()) {
  	 	  				if ((interfaceAddress.getAddress() instanceof Inet6Address)) {
  		        		  isIPv6 = true;
  	 	  				}
					}
  	 	  		}
  	 	  	}
    	} catch(Exception ex) {
			logger.error("isIPv6 Error: {}", ex.getMessage());
    	}
    	return isIPv6;
    }
    
    public static List<String> reachableAgents() {
    	List<String> rAgents = null;
    	try {
    		rAgents = new ArrayList<>();
    		if(isRegionalController) {
    			ActiveMQDestination[] er = ActiveBroker.broker.getBroker().getDestinations();
				for(ActiveMQDestination des : er) {
					if(des.isQueue()) {
						rAgents.add(des.getPhysicalName());
					}
				}
        	} else {
    			rAgents.add(region); //just return regional controller
    		}
    	} catch(Exception ex) {
			logger.error("isReachableAgent Error: {}", ex.getMessage());
    	}
    	return rAgents;
    }
    
    public static boolean isReachableAgent(String remoteAgentPath) {
    	boolean isReachableAgent = false;
    	if(isRegionalController) {
			try {
				ActiveMQDestination[] er = ActiveBroker.broker.getBroker().getDestinations();
				  for(ActiveMQDestination des : er) {
						if(des.isQueue()) {
							String testPath = des.getPhysicalName();
							if(testPath.equals(remoteAgentPath)) {
								isReachableAgent = true;
							}
						}
				  }
			} catch(Exception ex) {
				logger.error("isReachableAgent Error: {}", ex.getMessage());
			}
    	} else {
    		isReachableAgent = true; //send all messages to regional controller if not broker
    	}
    	return isReachableAgent;
    }
    
    public String getPluginName() //This should pull the version information from jar Meta data
    {
		   String name = "unknown";
		   try{
		   String jarFile = PluginImplementation.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		   File file = new File(jarFile.substring(5, (jarFile.length() -2)));
           FileInputStream fis = new FileInputStream(file);
           @SuppressWarnings("resource")
		   JarInputStream jarStream = new JarInputStream(fis);
		   Manifest mf = jarStream.getManifest();
		   
		   Attributes mainAttribs = mf.getMainAttributes();
           name = mainAttribs.getValue("artifactId");
		   }
		   catch(Exception ex)
		   {
			   String msg = "Unable to determine Plugin Version " + ex.toString();
			   logger.error(msg);
			   //clog.error(msg);
			   //version = "Unable to determine Version";
		   }
		   
		   return name;
	   }
	public String getPluginVersion() //This should pull the version information from jar Meta data
    {
		   String version = "unknown";
		   try{
		   String jarFile = PluginImplementation.class.getProtectionDomain().getCodeSource().getLocation().getPath();
		   File file = new File(jarFile.substring(5, (jarFile.length() -2)));
           FileInputStream fis = new FileInputStream(file);
           @SuppressWarnings("resource")
		   JarInputStream jarStream = new JarInputStream(fis);
		   Manifest mf = jarStream.getManifest();
		   
		   Attributes mainAttribs = mf.getMainAttributes();
           version = mainAttribs.getValue("Implementation-Version");
		   }
		   catch(Exception ex)
		   {
			   String msg = "Unable to determine Plugin Version " + ex.toString();
			   logger.error(msg);
			   //clog.error(msg);
			   //version = "Unable to determine Version";
		   }
		   
		   return version;
	   }
	

}