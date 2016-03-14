package plugincore;


import ActiveMQ.*;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import netdiscovery.DiscoveryClientIPv6;
import netdiscovery.DiscoveryEngine;
import shared.MsgEvent;
import shared.MsgEventType;
import shared.RandomString;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PluginEngine {
    
	public static boolean clientDiscoveryActive = false;
	public static boolean clientDiscoveryActiveIPv6 = false;
	public static boolean DiscoveryActive = false;
	public static boolean ActiveBrokerManagerActive = false;
	public static boolean ActiveDestManagerActive = false;
	public static boolean ConsumerThreadActive = false;
	public static boolean ConsumerThreadRegionActive = false;
	
	public static Thread discoveryEngineThread;
	public static Thread activeBrokerManagerThread;
	public static Thread consumerRegionThread;
	public static Thread consumerAgentThread;
	
	public static ActiveProducer ap;
	
	public static String brokerAddress;
	public static boolean isBroker = false;
	public static boolean isIPv6 = false;
	public static boolean isActive = true;
	
	public static int responds = 0;
	
	public static String region = "reg";
	public static String agent = "agent";
	public static String plugin = "pl";
	public static String agentpath;
	
	public static ConcurrentHashMap<String,BrokeredAgent> brokeredAgents;
	
	public static ConcurrentLinkedQueue<MsgEvent> incomingCanidateBrokers;
	public static ConcurrentLinkedQueue<MsgEvent> outgoingMessages;
	
	//public static DiscoveryClientIPv4 dc;
	public static DiscoveryClientIPv6 dcv6;
	
	
	public String getName()
	{
		return "Name";	
	}
	public String getVersion()
	{
		return "Version";
				
	}
	public void msgIn(MsgEvent command)
	{
		
	}
	public static void shutdown()
	{
		try
		{
			System.out.println("Shutting down!");
			DiscoveryActive = false;
			if(discoveryEngineThread != null)
			{
				System.out.println("discoveryEngineThread start");
				discoveryEngineThread.join();
				System.out.println("discoveryEngineThread shutdown");
				
			}
			
			ConsumerThreadRegionActive = false;
			if(consumerRegionThread != null)
			{
				System.out.println("consumerRegionThread start");
				consumerRegionThread.join();
				System.out.println("consumerRegionThread shutdown");
				
			}
			
			ConsumerThreadActive = false;
			if(consumerAgentThread != null)
			{
				System.out.println("consumerAgentThread start");
				
				consumerAgentThread.join();
				System.out.println("consumerAgentThread shutdown");
				
			}
			
			ActiveBrokerManagerActive = false;
			if(activeBrokerManagerThread != null)
			{
				System.out.println("activeBrokerManagerThread start");
				activeBrokerManagerThread.join();
				System.out.println("activeBrokerManagerThread shutdown");
				
			}
			if(broker != null)
			{
				System.out.println("broker start");
				broker.stopBroker();
				System.out.println("broker shutdown");
				
			}
		}
		catch(Exception ex)
		{
			System.out.println("PluginEngine : shutdown Error " + ex.getMessage());
		}
		
	}
	public boolean initialize(ConcurrentLinkedQueue<MsgEvent> msgOutQueue,ConcurrentLinkedQueue<MsgEvent> msgInQueue, SubnodeConfiguration configObj, String region,String agent, String plugin)  
	{
		return true;
	}
	
	public static ActiveBroker broker;
	
	
	
    public static void main(String[] args) throws Exception 
    {
    	
    	if(args.length == 1)
    	{
    		Thread.sleep(Integer.parseInt(args[0])*1000);
    	}
    	//Cleanup on Shutdown
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
	        public void run() {
	            try
	            {
					System.out.println("");
	            	System.out.println("Shutting down!");
	            	DiscoveryActive = false;
					//Thread.sleep(500);
	            	ConsumerThreadActive = false;
					//Thread.sleep(500);
	            	ActiveDestManagerActive = false;
					//Thread.sleep(500);
	            	ActiveBrokerManagerActive = false;
	            	
					Thread.sleep(1000);
	            	 
	            	
	            	broker.stopBroker();
	            	
	            }
	            catch(Exception ex)
	            {
	            	System.out.println("Exception Shutting Down:" + ex.toString());
	            }
	        }
	    }, "Shutdown-thread"));
		
    	
    	region = "region0";
    	RandomString rs = new RandomString(4);
		agent = "agent-" + rs.nextString();
		agentpath = region + "_" + agent;
    	
		isIPv6 = isIPv6();
    	
    	//peerList = new ArrayList<String>();
    	brokeredAgents = new ConcurrentHashMap<String,BrokeredAgent>(); 
    	
    	incomingCanidateBrokers = new ConcurrentLinkedQueue<MsgEvent>();
    	
    	outgoingMessages = new ConcurrentLinkedQueue<MsgEvent>();
    	
    	ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    	//rootLogger.setLevel(Level.toLevel("debug"));
    	//rootLogger.setLevel(Level.toLevel("none"));
    	rootLogger.setLevel(Level.ERROR);
    	
    	
    	
    	/*//disabled ipv4 discovery
    	//Start IPv4 network discovery engine
    	Thread de = new Thread(new DiscoveryEngine());
    	de.start();
    	while(!DiscoveryActive)
        {
        	//System.out.println("Wating on Discovery Server to start...");
        	Thread.sleep(1000);
        }
        System.out.println("IPv4 DiscoveryEngine Started..");
		*/
        
        dcv6 = new DiscoveryClientIPv6();
        //dc = new DiscoveryClientIPv4();
        
        try
        {
        	System.out.println("Broker Search IPv6:");
    		dcv6.getDiscoveryMap(2000);
        	//System.out.println("Broker Search IPv4:");
    		//dc.getDiscoveryMap(2000);
    		if(incomingCanidateBrokers.isEmpty())
    		{
    			//Start controller services
    			
    			//discovery engine
    			discoveryEngineThread = new Thread(new DiscoveryEngine());
    			discoveryEngineThread.start();
    	    	while(!DiscoveryActive)
    	        {
    	        	Thread.sleep(1000);
    	        }
    	        System.out.println("IPv6 DiscoveryEngine Started..");
    			
    	        //broker 
    	        broker = new ActiveBroker(agentpath);
    	        
    	        //broker manager
    	        activeBrokerManagerThread = new Thread(new ActiveBrokerManager());
    	        activeBrokerManagerThread.start();
    	    	while(!ActiveBrokerManagerActive)
    	        {
    	        	Thread.sleep(1000);
    	        }
    	        System.out.println("ActiveBrokerManager Started..");
    	        
    	        if(isIPv6) //set broker address for consumers and producers
    	    	{
    	    		brokerAddress = "[::1]";	
    	    	}
    	    	else
    	    	{
    	    		brokerAddress = "localhost";
    	    	}
    	        
    	        //consumer region 
    	        consumerRegionThread = new Thread(new ActiveRegionConsumer(region,"tcp://" + brokerAddress + ":32010"));
    	        consumerRegionThread.start();
    	    	while(!ConsumerThreadRegionActive)
    	        {
    	        	Thread.sleep(1000);
    	        }
    	        System.out.println("Region ConsumerThread Started..");
        		
    	        isBroker = true;
    	        
    		}
    		else
    		{
    			//determine least loaded broker
    			//need to use additional metrics to determine best fit broker
    			String cbrokerAddress = null;
    			int brokerCount = -1;
    			for(MsgEvent bm : incomingCanidateBrokers)
    			{
    				int tmpBrokerCount = Integer.parseInt(bm.getParam("agent_count"));
    				if(brokerCount < tmpBrokerCount)
    				{
    					cbrokerAddress = bm.getParam("dst_ip");
    				}
    			}
    			if(cbrokerAddress != null)
    			{
    				InetAddress remoteAddress = InetAddress.getByName(cbrokerAddress);
    				if(remoteAddress instanceof Inet6Address)
    				{
    					cbrokerAddress = "[" + cbrokerAddress + "]";
    				}
    				brokerAddress = cbrokerAddress;
    			}
    			
    		}
    		
    		//consumer agent 
	        consumerAgentThread = new Thread(new ActiveAgentConsumer(agentpath,"tcp://" + brokerAddress + ":32010"));
	        consumerAgentThread.start();
	    	while(!ConsumerThreadActive)
	        {
	        	Thread.sleep(1000);
	        }
	        System.out.println("Agent ConsumerThread Started..");
    		
	        ap = new ActiveProducer("tcp://" + brokerAddress + ":32010");
	        System.out.println("Producer Started..");
    		
    		
    	}
    	catch(Exception e)
    	{
    		System.out.println("PluginEngine : Main Error " + e.toString());
    	}

		System.out.println("Agent [" + agentpath + "] running...");

		Thread.sleep(2000);

		while (true) {
			System.out.print("Name of Agent to message: ");
			@SuppressWarnings("resource")
			Scanner scanner = new Scanner(System.in);
			String input = scanner.nextLine();
			if(input.length() == 0)
			{
				List<String> rAgents = reachableAgents();
				if(rAgents.isEmpty())
				{
					System.out.println("\tNo agents found... " + responds);
				}
				else
				{
					for(String str : rAgents)
					{
						System.out.println("\t" + str);
					}
					System.out.println("\tFound " + rAgents.size() + " agents");
					
					
				}
			}
			else
			{
				if(input.toLowerCase().equals("all"))
				{
					List<String> rAgents = reachableAgents();
					if(rAgents.isEmpty())
					{
						System.out.println("\tNo agents found...");
					}
					else
					{
						System.out.println("\tSending message to " + rAgents.size() + " agents");
						for(String str : rAgents)
						{
							System.out.println("\t"+str);
							sendMessage(MsgEventType.INFO, str, "ping");
						}
						System.out.println("\tSent message to " + rAgents.size() + " agents");
						
						
					}
				}
				else
				{
					sendMessage(MsgEventType.INFO, input, "ping");
				}
			}
		}
    }

    public static void commInit()
    {
    	//repeatable 
    	try
    	{
    		
    	}
    	catch(Exception ex)
    	{
    		
    	}
    }
    
    public static void sendMessage(MsgEventType type, String targetAgent, String msg) {
		if (isReachableAgent(targetAgent)) {
			System.out.println("Sending to Agent [" + targetAgent + "]");
			String[] str = targetAgent.split("_");
			MsgEvent sme = new MsgEvent(type, region, agent, plugin, msg);
			sme.setParam("src_region", region);
			sme.setParam("src_agent", agent);
			sme.setParam("dst_region", str[0]);
			if(str.length == 2) //send to region if agent does not exist
			{
				sme.setParam("dst_agent", str[1]);
			}
			ap.sendMessage(sme);
		} else {
			System.out.println("Cannot reach Agent [" + targetAgent + "]");
		}
	}
    
    
    public static boolean isLocal(String checkAddress)
    {
    	boolean isLocal = false;
    	
    	if(checkAddress.contains("%"))
    	{
    		String[] checkScope = checkAddress.split("%");
    		checkAddress = checkScope[0];
    	}
    	
    	List<String> localAddressList = localAddresses();
    	for(String localAddress : localAddressList)
    	{
    		//System.out.println("Checking: " + checkAddress + " localAddress: " + localAddress);
    		if(localAddress.contains(checkAddress))
    		{
    			isLocal = true;
    		}
    	}
    	return isLocal;
    }
    public static List<String> localAddresses()
    {
    	List<String> localAddressList = new ArrayList<String>();
    	try
    	{
    	Enumeration<NetworkInterface> inter = NetworkInterface.getNetworkInterfaces();
		  while (inter.hasMoreElements()) 
		  {
		    NetworkInterface networkInter = (NetworkInterface) inter.nextElement();
		    for (InterfaceAddress interfaceAddress : networkInter.getInterfaceAddresses()) 
		    {
		    	String localAddress = interfaceAddress.getAddress().getHostAddress();
		    	if(localAddress.contains("%"))
		    	{
		    		String[] localScope = localAddress.split("%");
		    		localAddress = localScope[0];
		    	}
		    	if(!localAddressList.contains(localAddress))
		    	{
		    		localAddressList.add(localAddress);
		    	}
		    }
		  }
    	}
    	catch(Exception ex)
    	{
    		System.out.println("PluginEngine : localAddresses Error " + ex.toString());
    	}
    	return localAddressList;
    }
    
    public static boolean isIPv6()
    {
    	boolean isIPv6 = false;
    	try
    	{
    		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
  	 	  	while (interfaces.hasMoreElements()) 
  	 	  	{
  	 	  		NetworkInterface networkInterface = (NetworkInterface) interfaces.nextElement();
  	 	  		if (networkInterface.getDisplayName().startsWith("veth") || networkInterface.isLoopback() || !networkInterface.isUp() || !networkInterface.supportsMulticast() || networkInterface.isPointToPoint() || networkInterface.isVirtual()) {
  	 	  			continue; // Don't want to broadcast to the loopback interface
  	 	  		}
  		    
  	 	  		if(networkInterface.supportsMulticast())
  	 	  		{
  	 	  			for (InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses())
  	 	  			{
  	 	  				if((interfaceAddress.getAddress() instanceof Inet6Address))
  	 	  				{
  		        		  isIPv6 = true;
  	 	  				}
  		        	 }
  	 	  		}
    		
  	 	  	}
    	}
    	catch(Exception ex)
    	{
    		System.out.println("PluginEngine : isIPv6 Error " + ex.toString());
    	}
    	return isIPv6;
    }
    
    public static List<String> reachableAgents()
    {
    	List<String> rAgents = null;
    	
    	try
    	{
    		rAgents = new ArrayList<String>();
    		
    		if(isBroker)
        	{
    		
    		ActiveMQDestination[] er = broker.broker.getBroker().getDestinations();
			  for(ActiveMQDestination des : er)
			  {
				  	if(des.isQueue())
					{
				  			rAgents.add(des.getPhysicalName());	
					}
			  }
        	}
    		else
    		{
    			rAgents.add(region); //just return regional controller
    		}
    	}
    	catch(Exception ex)
    	{
    		System.out.println("PluginEngine : isReachableAgent");
    	}
    	
    	return rAgents;
    }
    
    public static boolean isReachableAgent(String remoteAgentPath)
    {
    	boolean isReachableAgent = false;
    	if(isBroker)
    	{
    	try
    	{
    		ActiveMQDestination[] er = broker.broker.getBroker().getDestinations();
			  for(ActiveMQDestination des : er)
			  {
				  	if(des.isQueue())
					{
				  		String testPath = des.getPhysicalName();
				  		if(testPath.equals(remoteAgentPath))
				  		{
				  			isReachableAgent = true;				  			
			  				
				  			/*
				  			if(brokeredAgents.containsKey(remoteAgentPath))
				  			{
				  				if(brokeredAgents.get(remoteAgentPath).brokerStatus == BrokerStatusType.ACTIVE)
				  				{
				  					isReachableAgent = true;				  			
				  				}
				  			}
				  			*/
				  		}
				  		
					}
			  }
    	}
    	catch(Exception ex)
    	{
    		System.out.println("PluginEngine : isReachableAgent Error " + ex.getMessage());
    	}
    	}
    	else
    	{
    		//send all messages to regional controller if not broker
    		isReachableAgent = true;
    	}
    	return isReachableAgent;
    }
    
}