package ActiveMQ;

import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;

import plugincore.PluginEngine;
import shared.MsgEvent;


public class ActiveDestManager implements Runnable 
{
	//private MulticastSocket socket;
	private Timer timer;
	private MBeanServerConnection conn;
	public ActiveDestManager()
	{
		//timer = new Timer();
	    //timer.scheduleAtFixedRate(new BrokerWatchDog(), 500, 300000);//remote 
		//timer.scheduleAtFixedRate(new BrokerWatchDog(), 500, 15000);//remote
		try{
			System.out.println("test0");
		JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
		System.out.println("test1");
		                                
		JMXConnector jmxc = JMXConnectorFactory.connect(url);
		System.out.println("test2");
		
		conn = jmxc.getMBeanServerConnection();
		System.out.println("test3");
		
		PluginEngine.ActiveDestManagerActive = true;
	    
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
		while(PluginEngine.ActiveDestManagerActive)
	    {
		  try 
		  {
			  //String operationName="addQueue";
			  //String parameter="MyNewQueue";
			   
			  
			  ObjectName activeMQ = new ObjectName("org.apache.activemq:BrokerName=localhost,Type=Broker");
			  BrokerViewMBean mbean = (BrokerViewMBean) MBeanServerInvocationHandler.newProxyInstance(conn, activeMQ,BrokerViewMBean.class, true);
			  for (ObjectName name : mbean.getQueues()) 
			  {
				  System.out.println("Queue Name: " + name.getCanonicalName());
				  
			  }
			  Thread.sleep(3000);
			  /*
			  for (ObjectName name : mbean.getQueues()) {
			      QueueViewMBean queueMbean = (QueueViewMBean)
			             MBeanServerInvocationHandler.newProxyInstance(mbsc, name, QueueViewMBean.class, true);

			      if (queueMbean.getName().equals(queueName)) {
			          queueViewBeanCache.put(cacheKey, queueMbean);
			          return queueMbean;
			      }
			  } 
			  */
			  
		  } 
		  catch (Exception ex) 
		  {
			  System.out.println("ActiveDestManager : Run Error " + ex.toString());
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