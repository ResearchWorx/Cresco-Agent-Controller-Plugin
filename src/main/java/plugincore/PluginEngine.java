package plugincore;


import org.apache.commons.configuration.SubnodeConfiguration;
import org.slf4j.LoggerFactory;

import ActiveMQ.ActiveBroker;
import ActiveMQ.ActiveBrokerManager;
import ActiveMQ.ActiveConsumer;
import ActiveMQ.ActiveProducer;
import ActiveMQ.BrokeredAgent;
import ch.qos.logback.classic.Level;
import netdiscovery.DiscoveryClientIPv4;
import netdiscovery.DiscoveryResponder;
import netdiscovery.DiscoveryClientIPv6;
import netdiscovery.DiscoveryEngine;
import shared.MsgEvent;
import shared.MsgEventType;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PluginEngine {
    
	public static boolean clientDiscoveryActive = false;
	public static boolean clientDiscoveryActiveIPv6 = false;
	public static boolean DiscoveryActive = false;
	public static boolean DiscoveryResponderActive = false;
	public static boolean ActiveBrokerManagerActive = false;
	
	
	public static boolean isIPv6 = false;
	
	public static boolean isActive = true;
	
	public static String region = "reg";
	public static String agent = "agent";
	public static String plugin = "pl";
	public static String agentpath;
	
	public static ConcurrentHashMap<String,BrokeredAgent> brokeredAgents;
	
	public static ConcurrentLinkedQueue<MsgEvent> discoveryResponse;
	public static ConcurrentLinkedQueue<MsgEvent> incomingCanidateBrokers;
	
	public static DiscoveryClientIPv4 dc;
	public static DiscoveryClientIPv6 dcv6;
	
	//public static List<String> peerList;
	
	public String getName()
	{
		return "Name";
				
	}
	public String getVersion()
	{
		return "Name";
				
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
    	isIPv6 = isIPv6();
    	
    	region = args[0];
    	agent = args[1];
    	agentpath = region + "_" + agent;
    	
    	//peerList = new ArrayList<String>();
    	brokeredAgents = new ConcurrentHashMap<String,BrokeredAgent>(); 
    	
    	discoveryResponse = new ConcurrentLinkedQueue<MsgEvent>();
    	incomingCanidateBrokers = new ConcurrentLinkedQueue<MsgEvent>();
    	
    	ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    	//rootLogger.setLevel(Level.toLevel("debug"));
    	//rootLogger.setLevel(Level.toLevel("none"));
    	rootLogger.setLevel(Level.WARN);

    	broker = new ActiveBroker(agentpath);
    	
    	Thread abm = new Thread(new ActiveBrokerManager());
    	abm.start();
    	while(!ActiveBrokerManagerActive)
        {
        	Thread.sleep(1000);
        }
        System.out.println("ActiveBrokerManager Started..");
		
    	
    	//Thread ct = new Thread(new ActiveConsumer(agentpath,"tcp://localhost:32010"));
    	//Thread ct = new Thread(new ActiveConsumer(args[1],"tcp://[::1]:32010"));
    	//ct.start();
    	
    	
    	//Thread pt = new Thread(new ActiveProducer(args[2] + "_" + args[3],"tcp://localhost:32010"));
    	//Thread pt = new Thread(new ActiveProducer(args[2],"tcp://[::1]:32010"));
    	//pt.start();
    	
    	//Start discovery broker
    	Thread db = new Thread(new DiscoveryResponder());
    	db.start();
    	while(!DiscoveryResponderActive)
        {
        	Thread.sleep(1000);
        }
        System.out.println("DiscoveryBroker Started..");
		
    	
    	//Start IPv6 network discovery engine
    	Thread dev6 = new Thread(new DiscoveryEngine());
    	dev6.start();
    	while(!DiscoveryActive)
        {
        	Thread.sleep(1000);
        }
        System.out.println("IPv6 DiscoveryEngine Started..");
		
    	/*
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
        dc = new DiscoveryClientIPv4();
        
        try{
        	int processCount = 0;
        	
        	System.out.println("Broker Search IPv6:");
    		processCount = processPeerMap(dcv6.getDiscoveryMap(2000));
    		
    		
    		System.out.println("Broker Search IPv4:");
    		processCount = processPeerMap(dc.getDiscoveryMap(2000));
    		
    		if(processCount > 0)
      		{
      			System.out.println("Neighbor Exists");
      		}
    		else
    		{
      			System.out.println("Better start something Brah");
      		}
    	}
    	catch(Exception e)
    	{
    		System.out.println("PluginEngine : Main Error " + e.toString());
    	}
        
       
    }
    
    public static int processPeerMap(List<MsgEvent> disList)
    {
    	int processCount = 0;
    	if(!disList.isEmpty())
		{
    		for(MsgEvent db : disList)
    		{
    			incomingCanidateBrokers.offer(db);
    			processCount++;
    		}
			
		}
    	return processCount;
    	
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
    
}