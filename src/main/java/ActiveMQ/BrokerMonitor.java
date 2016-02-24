package ActiveMQ;

import java.net.Inet6Address;
import java.net.InetAddress;

import org.apache.activemq.network.NetworkConnector;

import plugincore.PluginEngine;

class BrokerMonitor implements Runnable 
{
	  private String agentPath;
	  public boolean MonitorActive;
	  public BrokerMonitor(String agentPath)
	  {
	    	this.agentPath = agentPath;
	    	this.MonitorActive = true;
	  }
	  public void shutdown()
	  {
			MonitorActive = false;
	  }
	  public void run() 
	  {
		  try
		  {
			  String brokerAddress = PluginEngine.brokeredAgents.get(agentPath).activeAddress;
			  
			  if((InetAddress.getByName(brokerAddress) instanceof Inet6Address))
        	  {
				  brokerAddress = "[" + brokerAddress + "]";
        	  }
			  NetworkConnector bridge = PluginEngine.broker.AddNetworkConnector(brokerAddress);
			  bridge.start();
			    //PluginEngine.broker.AddNetworkConnector(URI)
	    		while(MonitorActive)
	    		{
	    			System.out.println("Monitoring thread for : " + agentPath);
	    			System.out.println("Started : " + bridge.isStarted());
	    			System.out.println("Static : " + bridge.isStaticBridge());
	    			System.out.println("Name : " + bridge.getDestinationFilter());
	    			
	    			Thread.sleep(5000);
	    			bridge.stop();
	    			PluginEngine.brokeredAgents.get(agentPath).brokerStatus = BrokerStatusType.FAILED;
	    			MonitorActive = false;
	    		}
		  }
		  catch(Exception ex)
		  {
	    		
		  }
	  }
}