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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
	
	public static DiscoveryClient dc;
	public static DiscoveryClientIPv6 dcv6;
	
	public static List<String> peerList;
	
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
    	peerList = new ArrayList<String>();
    	
    	ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    	//rootLogger.setLevel(Level.toLevel("debug"));
    	//rootLogger.setLevel(Level.toLevel("none"));
    	rootLogger.setLevel(Level.WARN);

    	broker = new ActiveBroker(args[0]);
    	//tcp://localhost:32010
    	Thread ct = new Thread(new ActiveConsumer(args[1],"tcp://localhost:32010"));
    	ct.start();
    	
    	Thread pt = new Thread(new ActiveProducer(args[2],"tcp://localhost:32010"));
    	pt.start();
    	
    	//Start IPv4 network discovery engine
    	Thread de = new Thread(new DiscoveryEngine());
    	de.start();
    	while(!DiscoveryActive)
        {
        	//System.out.println("Wating on Discovery Server to start...");
        	Thread.sleep(1000);
        }
        System.out.println("IPv4 DiscoveryEngine Started..");
		
        
        //Start IPv6 network discovery engine
    	Thread dev6 = new Thread(new DiscoveryEngineIPv6());
    	dev6.start();
    	while(!DiscoveryActiveIPv6)
        {
        	//System.out.println("Wating on Discovery Server to start...");
        	Thread.sleep(1000);
        }
        System.out.println("IPv6 DiscoveryEngine Started..");
		
        
        dc = new DiscoveryClient();
        dcv6 = new DiscoveryClientIPv6();
        
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
    			
    		for (Map.Entry<String, String> entry : bmap.entrySet())
    		{
    		    //System.out.println(entry.getKey() + "/" + entry.getValue());
    		    String[] str2 = entry.getValue().split(",");
    		
    		    for(int i = 1; i < str2.length; i++)
    		    {
    		    	String[] str3 = str2[i].split("_");
    		    	if(!peerList.contains(str3[1]))
    		    	{
    		    		processPeer(str3[1]);
    		    	}
    		    }
    		    
    		}
    	    
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    	}
       
    }
    public static void processPeer(String peer)
    {
    	try
    	{
    		System.out.println("ProcessPeer: " + peer);
    		InetAddress peerAddress = InetAddress.getByName(peer);
    		boolean isReachable = false;
    		
    		if(peerAddress instanceof Inet6Address)
    		{
    			isReachable = dcv6.isReachable(peer);
    			
    		}
    		else if(peerAddress instanceof Inet4Address)
    		{
    			isReachable = dc.isReachable(peer);
    		}
    		
    		if(isReachable)
    		{
    			System.out.println("ProcessPeer: " + peer + " is reachable");
    			peerList.add(peer);
    		}
    	}
    	catch(Exception ex)
    	{
    		System.out.println(ex.toString());
    	}
    }
}