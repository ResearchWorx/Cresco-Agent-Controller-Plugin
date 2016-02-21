package plugincore;


import org.apache.commons.configuration.SubnodeConfiguration;
import org.slf4j.LoggerFactory;

import ActiveMQ.ActiveBroker;
import ActiveMQ.ActiveConsumer;
import ActiveMQ.ActiveProducer;
import ch.qos.logback.classic.Level;
import netdiscoveryIPv4.DiscoveryClient;
import netdiscoveryIPv4.DiscoveryEngine;
import netdiscoveryIPv6.DiscoveryClientIPv6;
import netdiscoveryIPv6.DiscoveryEngineIPv6;
import shared.MsgEvent;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PluginEngine {
    
	public static boolean clientDiscoveryActive = false;
	public static boolean clientDiscoveryActiveIPv6 = false;
	public static boolean DiscoveryActive = false;
	public static boolean DiscoveryActiveIPv6 = false;
	
	public static boolean isActive = true;
	
	public static String region = "reg";
	public static String agent = "agent";
	public static String plugin = "pl";
	public static String agentpath;
	
	public static ConcurrentHashMap<String,String> abhm;
	public static ConcurrentHashMap<String,String> pbhm;
	
	public static DiscoveryClient dc;
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
    	region = args[0];
    	agent = args[1];
    	agentpath = region + "_" + agent;
    	
    	//peerList = new ArrayList<String>();
    	abhm = new ConcurrentHashMap<String,String>(); 
    	pbhm = new ConcurrentHashMap<String,String>(); 
    	
    	ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    	//rootLogger.setLevel(Level.toLevel("debug"));
    	//rootLogger.setLevel(Level.toLevel("none"));
    	rootLogger.setLevel(Level.WARN);

    	broker = new ActiveBroker(agentpath);
    	
    	
    	Thread ct = new Thread(new ActiveConsumer(agentpath,"tcp://localhost:32010"));
    	//Thread ct = new Thread(new ActiveConsumer(args[1],"tcp://[::1]:32010"));
    	ct.start();
    	
    	
    	Thread pt = new Thread(new ActiveProducer(args[2] + "_" + args[3],"tcp://localhost:32010"));
    	//Thread pt = new Thread(new ActiveProducer(args[2],"tcp://[::1]:32010"));
    	pt.start();
    	
    	//Start IPv6 network discovery engine
    	Thread dev6 = new Thread(new DiscoveryEngineIPv6());
    	dev6.start();
    	while(!DiscoveryActiveIPv6)
        {
        	//System.out.println("Wating on Discovery Server to start...");
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
        
        dc = new DiscoveryClient();
        dcv6 = new DiscoveryClientIPv6();
        
        try{
        	boolean foundNeighbor = false;
        	
        	System.out.println("Broker Search IPv6:");
    		if(processPeerMap(dcv6.getDiscoveryMap(6000)))
    		{
    			//Add all IPv6 Addresses
    			foundNeighbor = true;
    		}
    		/*
    		System.out.println("Broker Search IPv4:");
    		if(processPeerMap(dc.getDiscoveryMap(2000)))
    		{
    			foundNeighbor = true;
    			//Add all IPv6 Addresses
    		}
    		*/
    		if(foundNeighbor)
      		{
      			System.out.println("Neighbor Exists");
      		}
    		
    		if(!foundNeighbor)
      		{
      			System.out.println("Better start something Brah");
      		}
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
        
        
        /*
        while(true)
    	{
    	try{
    		System.out.println("Broker Search:");
    		BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
    	    String s = bufferRead.readLine();
    	    String str[] = s.split(" ");
    	    Map<String,String> bmap = null;
    		if(str[0].equals("6"))
    		{
    			bmap = dcv6.getDiscoveryMap(Integer.parseInt(str[1]));
    		}
    		else if(str[0].equals("4"))
    		{
    			bmap = dc.getDiscoveryMap(Integer.parseInt(str[1]));
    		}
    		
    		if(!bmap.isEmpty())
    		{
    			String[] discoveredAgents = bmap.get("discoveredagents").split(",");
    			for(String discoveredAgent : discoveredAgents)
    			{
    				String[] agentNetwork = bmap.get(discoveredAgent).split("_");
    				processPeer(agentNetwork[1]);
    		    }
    		}	
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    	}
        */
        
        //end
       
    }
    
    public static boolean processPeerMap(Map<String,String> bmap)
    {
    	boolean foundNeighbor = false;
    	if(!bmap.isEmpty())
		{
  			String[] discoveredAgents = bmap.get("discoveredagents").split(",");
			for(String discoveredAgent : discoveredAgents)
			{
				String[] agentNetworks = bmap.get(discoveredAgent).split(",");
				for(String agentNetwork : agentNetworks)
				{
					String[] agentNetworkHosts = agentNetwork.split("_");
					if(processPeer(agentNetworkHosts[1],discoveredAgent))
					{
						foundNeighbor = true;
					}
				}
		    }
		}
    	return foundNeighbor;
    }
    
    public static List<String> localAddresses()
    {
    	List<String> localAddressList = new ArrayList<String>();
    	try
    	{
    	Enumeration inter = NetworkInterface.getNetworkInterfaces();
		  while (inter.hasMoreElements()) 
		  {
		    NetworkInterface networkInter = (NetworkInterface) inter.nextElement();
		    for (InterfaceAddress interfaceAddress : networkInter.getInterfaceAddresses()) 
		    {
		    	String localAddress = interfaceAddress.getAddress().getHostAddress();
		    	localAddressList.add(localAddress);
		    
		    }
		  }
    	}
    	catch(Exception ex)
    	{
    		System.out.println("PluginEngine : localAddresses Error " + ex.toString());
    	}
    	return localAddressList;
    }
    public static boolean processPeer(String peer,String agentpath)
    {
    	boolean isPeer = false;
    	boolean isIPv6 = false;
    	try
    	{
    		if((!localAddresses().contains(peer) && (!abhm.containsKey(peer))) && (!pbhm.containsKey(agentpath)))
    		{
    			if(peer.contains("%"))
				{
					String[] peerScope = peer.split("%");
					peer = peerScope[0];
				}
    			System.out.println("ProcessPeer: " + peer);
    			InetAddress peerAddress = InetAddress.getByName(peer);
    			boolean isReachable = false;
    		
    			if(peerAddress instanceof Inet6Address)
    			{
    				isReachable = dcv6.isReachable(peer);
    				isIPv6 = true;
    			}
    			else if(peerAddress instanceof Inet4Address)
    			{
    				isReachable = dc.isReachable(peer);
    			}
    		
    			if(isReachable)
    			{
    				System.out.println("ProcessPeer: " + peer + " is reachable");
    				System.out.println("adding network connect for peer: " + peer);
    				if(isIPv6)
    				{
    					broker.AddNetworkConnector("[" + peer + "]");
        			}
    				else
    				{
    					broker.AddNetworkConnector(peer);
        			}
    				//peerList.add(peer);
    				abhm.put(peer,agentpath);
    				pbhm.put(agentpath,peer);
    				
    				isPeer = true;
    			}
    		}
    	}
    	catch(Exception ex)
    	{
    		System.out.println(ex.toString());
    	}
    	return isPeer;
    }
}