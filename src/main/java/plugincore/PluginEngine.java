package plugincore;


import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.*;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.slf4j.LoggerFactory;

import ActiveMQ.ActiveBroker;
import ActiveMQ.ActiveConsumer;
import ActiveMQ.ActiveProducer;
import ch.qos.logback.classic.Level;
import netdiscovery.DiscoveryClient;
import netdiscovery.DiscoveryEngine;
import shared.MsgEvent;

import javax.jms.*;
import javax.jms.Queue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PluginEngine {
    
	public static boolean clientDiscoveryActive = false;
	public static boolean DiscoveryActive = false;
	public static boolean isActive = false;
	public static boolean NetBenchEngineActive = false;
	
	public static String region = "reg";
	public static String agent = "agent";
	public static String plugin = "pl";
	
	public static DiscoveryClient dc;
	
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
    	
    	
    	//Start network discovery engine
    	Thread de = new Thread(new DiscoveryEngine());
    	de.start();
    	while(!DiscoveryActive)
        {
        	//System.out.println("Wating on Discovery Server to start...");
        	Thread.sleep(1000);
        }
        System.out.println("DiscoveryEngine Started..");
		
        dc = new DiscoveryClient();
        
        while(true)
    	{
    	try{
    		System.out.println("Broker Search:");
    		BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
    	    String s = bufferRead.readLine();
    	    
    		Map<String,String> bmap = dc.getDiscoveryMap(Integer.parseInt(s));
    		for (Map.Entry<String, String> entry : bmap.entrySet())
    		{
    		    System.out.println(entry.getKey() + "/" + entry.getValue());
    		}
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    	}
        
    	/*
    	while(true)
    	{
    	try{
    		System.out.println("Enter Broker IP:");
    	    BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
    	    String s = bufferRead.readLine();
    	    broker.AddNetworkConnector(s);
    	    System.out.println(s);
    	}
    	catch(IOException e)
    	{
    		e.printStackTrace();
    	}
    	}
    	*/
    }  
}