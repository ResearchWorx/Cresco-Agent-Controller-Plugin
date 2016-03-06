package plugincore;


import ActiveMQ.*;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import netdiscovery.DiscoveryClientIPv4;
import netdiscovery.DiscoveryClientIPv6;
import netdiscovery.DiscoveryEngine;
import shared.MsgEvent;
import shared.MsgEventType;
import shared.RandomString;

import java.net.Inet6Address;
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
	public static ActiveProducer ap;
	
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
	public void shutdown()
	{
		
	}
	public boolean initialize(ConcurrentLinkedQueue<MsgEvent> msgOutQueue,ConcurrentLinkedQueue<MsgEvent> msgInQueue, SubnodeConfiguration configObj, String region,String agent, String plugin)  
	{
		return true;
	}
	
	public static ActiveBroker broker;
    public static void main(String[] args) throws Exception 
    {
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
    	
    	
    	//disable everything related to broker
    	/*
    	broker = new ActiveBroker(agentpath);
    	
    	Thread abm = new Thread(new ActiveBrokerManager());
    	abm.start();
    	while(!ActiveBrokerManagerActive)
        {
        	Thread.sleep(1000);
        }
        System.out.println("ActiveBrokerManager Started..");
        
        
        Thread adm;
        if(isIPv6)
    	{
        	adm = new Thread(new ActiveDestManager("tcp://[::1]:32010"));
    	}
    	else
    	{
    		adm = new Thread(new ActiveDestManager("tcp://localhost:32010"));
    	}
        adm.start();
        while(!ActiveDestManagerActive)
        {
        	Thread.sleep(1000);
        }
        System.out.println("ActiveDestManager Started..");
        
        
        
        Thread ct = null;
    	if(isIPv6)
    	{
    		ct = new Thread(new ActiveConsumer(agentpath,"tcp://[::1]:32010"));
    	}
    	else
    	{
    		ct = new Thread(new ActiveConsumer(agentpath,"tcp://localhost:32010"));
    	}
    	ct.start();
    	while(!ConsumerThreadActive)
        {
        	Thread.sleep(1000);
        }
        System.out.println("ConsumerThread Started..");
        
        if(isIPv6)
    	{
    		ap = new ActiveProducer("tcp://[::1]:32010");
    	}
    	else
    	{
    		ap = new ActiveProducer("tcp://localhost:32010");
    		
    	}
        */
    	
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
        
        try{
        	System.out.println("Broker Search IPv6:");
    		dcv6.getDiscoveryMap(2000);
        	//System.out.println("Broker Search IPv4:");
    		//dc.getDiscoveryMap(2000);
    		if(incomingCanidateBrokers.isEmpty())
    		{
    			//Start IPv6 network discovery engine
    	    	Thread dev6 = new Thread(new DiscoveryEngine());
    	    	dev6.start();
    	    	while(!DiscoveryActive)
    	        {
    	        	Thread.sleep(1000);
    	        }
    	        System.out.println("IPv6 DiscoveryEngine Started..");
    			
    		}
    		
    		
    	}
    	catch(Exception e)
    	{
    		System.out.println("PluginEngine : Main Error " + e.toString());
    	}

		System.out.println("Agent [" + agentpath + "] running...");

		Thread.sleep(2000);

		while (true) {
			System.out.print("Name of Agent to message: ");
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
					System.out.println("\tFound " + rAgents.size() + " agents");
					for(String str : rAgents)
					{
						System.out.println("\t" + str);
					}
					
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
							sendMessage(MsgEventType.INFO, str, "Test Message!");
						}
						
					}
				}
				else
				{
					sendMessage(MsgEventType.INFO, input, "Test Message!");
				}
			}
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
			sme.setParam("dst_agent", str[1]);
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
    		
    		ActiveMQDestination[] er = broker.broker.getBroker().getDestinations();
			  for(ActiveMQDestination des : er)
			  {
				  	if(des.isQueue())
					{
				  		rAgents.add(des.getPhysicalName());
					}
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
    		System.out.println("PluginEngine : isReachableAgent");
    	}
    	
    	
    	return isReachableAgent;
    }
    
}