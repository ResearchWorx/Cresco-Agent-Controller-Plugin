package ActiveMQ;

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
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;

import plugincore.PluginEngine;
import shared.MsgEvent;


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
			
		while(PluginEngine.ActiveDestManagerActive)
	    {
			  System.out.println("Checking Queues START");
				
		  try 
		  {
			  System.out.println("Checking Queues LOOP");
			  DestinationSource destinationSource = activeMQConnection.getDestinationSource();

			  ActiveMQDestination[] er = PluginEngine.broker.broker.getDestinations();
			  for(ActiveMQDestination des : er)
			  {
				 System.out.println("Dest: " + des.getPhysicalName());
			    	for(String path : des.getDestinationPaths())
			    	{
			    		System.out.println("DES PATH: " + path);
			    		
			    	}
			  }
			    Set<ActiveMQQueue> queues = destinationSource.getQueues();
			    for(ActiveMQQueue queue : queues)
			    {
			    	System.out.println("Queue: " + queue.getPhysicalName());
			    	for(String path : queue.getDestinationPaths())
			    	{
			    		System.out.println("PATH: " + path);
			    		
			    	}
			    	
			    }
			  Thread.sleep(3000);
		  } 
		  catch (Exception ex) 
		  {
			  System.out.println("ActiveDestManager : Run Error " + ex.toString());
		  }
	    }
		  System.out.println("Checking Queues EXIT");
			
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