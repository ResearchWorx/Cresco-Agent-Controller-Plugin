package ActiveMQ;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.List;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.network.NetworkBridge;
import org.apache.activemq.network.NetworkConnector;

import plugincore.PluginEngine;

class BrokerMonitor implements Runnable 
{
	  private String agentPath;
	  private NetworkConnector bridge;
	  public boolean MonitorActive;
	  public BrokerMonitor(String agentPath)
	  {
	    	this.agentPath = agentPath;
	    	this.MonitorActive = true;
	  }
	  public void shutdown()
	  {
		    //failBridge();
		  System.out.println("CODY!!! BrokerMonitor : agentPath : " + agentPath + " CODY!!!");
		  stopBridge(); //kill bridge
		  MonitorActive = false;
	  }
	  public boolean connectToBroker(String brokerAddress)
	  {
		  boolean isConnected = false;
		  try
		  {

			  if((InetAddress.getByName(brokerAddress) instanceof Inet6Address))
        	  {
				  brokerAddress = "[" + brokerAddress + "]";
        	  }
			  bridge = PluginEngine.broker.AddNetworkConnector(brokerAddress);
			  bridge.start();
			  int connect_count = 0;
			  while((connect_count < 10) && !bridge.isStarted())
			  {
				  Thread.sleep(1000);
			  }
			  connect_count = 0;
			  
			  while((connect_count < 10) && !isConnected)
			  {
				  for(NetworkBridge b : bridge.activeBridges())
  				  {
					String remoteBroker = b.getRemoteBrokerName();
					//System.out.println("Try: " + connect_count);
					//System.out.println("local address: " + b.getLocalAddress());
					//System.out.println("localbrokername: " + b.getLocalBrokerName());
					//System.out.println("remoteaddress: " + b.getRemoteAddress());
					//System.out.println("remotebrokerid: " + b.getRemoteBrokerId());
					//System.out.println("remotebrokername: "+ b.getRemoteBrokerName());
					connect_count++;
					if(remoteBroker != null)
					{
						if(remoteBroker.equals(agentPath))
	    				{
							System.out.println("New Network Broker:");
							System.out.println("localbrokername: " + b.getLocalBrokerName());
							System.out.println("remoteaddress: " + b.getRemoteAddress());
							System.out.println("remotebrokerid: " + b.getRemoteBrokerId());
							System.out.println("remotebrokername: "+ b.getRemoteBrokerName());
							
	    					isConnected = true;
	    				}
					}
					
					Thread.sleep(1000);
  				  }
			  }
		  }
		  catch(Exception ex)
		  {
			System.out.println(getClass().getName() + " connectToBroker Error " + ex.toString());
		  }
		  return isConnected;
	  }
	  public void stopBridge()
	  {
		  System.out.println("Failed Bridge : " + agentPath);
			
		   try {
			PluginEngine.broker.removeNetworkConnector(bridge);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		   PluginEngine.brokeredAgents.get(agentPath).brokerStatus = BrokerStatusType.FAILED;
		   MonitorActive = false;
		   
	  }
	  public void run() 
	  {
		  try
		  {
			  String brokerAddress = PluginEngine.brokeredAgents.get(agentPath).activeAddress;
			  
			  if(!connectToBroker(brokerAddress)) //connect to broker
			  {
				   failBridge();    
			  }
			    while(MonitorActive)
	    		{
			    	/*
	    			System.out.println("Monitoring thread for : " + agentPath);
	    			System.out.println("Started : " + bridge.isStarted());
	    			System.out.println("Static : " + bridge.isStaticBridge());
	    			System.out.println("Broker Name : " + bridge.getBrokerName());
	    			System.out.println("Name : " + bridge.getName());
	    			System.out.println("duplex : " + bridge.isDuplex());
	    			System.out.println("compress : " + bridge.isUseCompression());
	    			*/
	    			//int count = 0;
			    	
	    			boolean bridgeActive = false;
	    			for(NetworkBridge b : bridge.activeBridges())
	    			{
	    				/*
	    				System.out.println("Active Bridge: " + count);
	    				System.out.println("local address: " + b.getLocalAddress());
	    				System.out.println("localbrokername: " + b.getLocalBrokerName());
	    				System.out.println("remoteaddress: " + b.getRemoteAddress());
	    				System.out.println("remotebrokerid: " + b.getRemoteBrokerId());
	    				System.out.println("remotebrokername: "+ b.getRemoteBrokerName());
	    				*/
	    				//count++;
	    				if(b.getRemoteBrokerName().equals(agentPath))
	    				{
	    					bridgeActive = true;
	    				}
	    			}
	    			if(!bridgeActive)
	    			{
	    				failBridge(); 
	    			}
	    			Thread.sleep(5000);
	    			
	    		}
		  }
		  catch(Exception ex)
		  {
	    		
		  }
	  }
}