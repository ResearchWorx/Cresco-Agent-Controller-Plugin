package ActiveMQ;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.advisory.DestinationSource;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;

import plugincore.PluginEngine;
import shared.MsgEvent;
import shared.MsgEventType;


public class ActiveDestManager implements Runnable 
{
	//private MulticastSocket socket;
	private Timer timer;
	private ActiveMQConnection activeMQConnection;
	
	public ActiveDestManager(String URI)
	{
		
		//timer = new Timer();
	    //timer.scheduleAtFixedRate(new BrokerWatchDog(), 500, 300000);//remote 
		//timer.scheduleAtFixedRate(new BrokerWatchDog(), 500, 15000);//remote
		try{
			ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(URI);
			//Connection conn = factory.createConnection();
			//conn.start();
			activeMQConnection = (ActiveMQConnection) factory.createConnection();
		    activeMQConnection.start();
		
		}
		catch(Exception ex)
		{
			System.out.println("ActiveDestManager Init : Run Error " + ex.toString());
			PluginEngine.ActiveDestManagerActive = false;
		}
	}
	  
	public void shutdown()
	{
	
	}
		  
	  public void run() 
	  {
		  PluginEngine.ActiveDestManagerActive = true;
		    
		  System.out.println("Checking Queues");
			
		//List<String> agentList = new ArrayList<String>();
		//Map<String,ActiveProducer> cm = new HashMap<String,ActiveProducer>();
		while(PluginEngine.ActiveDestManagerActive)
	    {
			int count = 0; //count queues
			
		  try 
		  {
			  
			  ActiveMQDestination[] er = PluginEngine.broker.broker.getBroker().getDestinations();
			  for(ActiveMQDestination des : er)
			  {
				 	if(des.isQueue())
					{
						boolean reachable = false;
				 		count++;
						for(String path : des.getDestinationPaths())
						{
							if(!des.getPhysicalName().equals(PluginEngine.agentpath))
							{
								reachable = true;
								System.out.println("Dest: " + des.getPhysicalName() + " is reachable = " + PluginEngine.isReachableAgent(des.getPhysicalName()));

								String[] str = des.getPhysicalName().split("_");
								MsgEvent sme = new MsgEvent(MsgEventType.INFO,PluginEngine.region,PluginEngine.agent,PluginEngine.plugin,"Discovery request.");
								sme.setParam("src_region",PluginEngine.region);
								sme.setParam("src_agent",PluginEngine.agent);
								sme.setParam("dst_region",str[0]);
								sme.setParam("dst_agent",str[1]);
								  
								//
								
								//PluginEngine.outgoingMessages.offer(sme);
								if (!PluginEngine.ap.sendMessage(sme)) {
									System.out.println("Message send failure!");
								}
								//if(!cm.containsKey(des.getPhysicalName()))
								//{
								/*
									if(PluginEngine.isReachableAgent(des.getPhysicalName()))
									{
										//System.out.println("Dest: " + des.getPhysicalName() + "starting feed");
										//ActiveProducer ap = new ActiveProducer(des.getPhysicalName(),"tcp://[::1]:32010");
										//cm.put(des.getPhysicalName(), ap);
										//new Thread(ap).start();
										
									}
									*/
								//}
									/*
								else
								{
									if(!PluginEngine.isReachableAgent(des.getPhysicalName()))
									{
										
										ActiveProducer ap = cm.get(des.getPhysicalName());
										ap.ActiveProducer = false;
										System.out.println("Dest: " + des.getPhysicalName() + "stopping feed");
										
										cm.remove(des.getPhysicalName());
										
									}
								}
								*/
								//System.out.println("DES PATH: " + path);
							}
			    		}
						if (!reachable) {
							System.out.println(des.getPhysicalName() + " is unreachable");
						}
					}
			  }
			   /*
			  DestinationSource destinationSource = activeMQConnection.getDestinationSource();

			    Set<ActiveMQQueue> queues = destinationSource.getQueues();
			    for(ActiveMQQueue queue : queues)
			    {
			    	System.out.println("Queue: " + queue.getPhysicalName());
			    	for(String path : queue.getDestinationPaths())
			    	{
			    		System.out.println("PATH: " + path);
			    		
			    	}
			    	
			    }
			    */
			  Thread.sleep(3000);
		  } 
		  catch (Exception ex) 
		  {
			  ex.printStackTrace();
			  System.out.println("ActiveDestManager : Run Error " + ex.toString());
			  PluginEngine.ActiveDestManagerActive = false;
		  }
	    }
		  
	  }
	
		class BrokerWatchDog extends TimerTask {
		    public void run() 
		    {
		    	for (Entry<String, BrokeredAgent> entry : PluginEngine.brokeredAgents.entrySet())
		    	{
		    	    //System.out.println(entry.getKey() + "/" + entry.getValue());
		    		BrokeredAgent ba = entry.getValue();
		    		if(ba.brokerStatus == BrokerStatusType.FAILED)
		    		{
		    			//System.out.println("stopping agentPath: " + ba.agentPath);
		    			ba.setStop();
		    			System.out.println("Cleared agentPath: " + ba.agentPath);
		    			PluginEngine.brokeredAgents.remove(entry.getKey());//remove agent
				    	
		    		}
		    		
		    	}
		    }
		  }

	  
}