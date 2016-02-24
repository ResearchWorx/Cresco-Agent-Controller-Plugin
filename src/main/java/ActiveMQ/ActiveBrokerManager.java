package ActiveMQ;


import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.util.TimerTask;

import plugincore.PluginEngine;
import shared.MsgEvent;


public class ActiveBrokerManager implements Runnable 
{
	//private MulticastSocket socket;
	
	public ActiveBrokerManager()
	{
	
	}
	  
	public void shutdown()
	{
	
	}
	

	  public void addBroker(String agentPath)
	  {
		  BrokeredAgent ba = PluginEngine.brokeredAgents.get(agentPath);
		  if(ba.brokerStatus == BrokerStatusType.INIT)
		  {
			  //Fire up new thread.
			  //ba.brokerStatus = BrokerStatusType.STARTING;
			  //System.out.println("Adding Broker: " + agentPath + " IP:" + ba.activeAddress);
			  ba.setStarting();
		  }
	  }
	  
	  public void run() 
	  {
		PluginEngine.ActiveBrokerManagerActive = true;
	    while(PluginEngine.ActiveBrokerManagerActive)
	    {
		  try 
		  {
			  MsgEvent cb = PluginEngine.incomingCanidateBrokers.poll();
			  if(cb != null)
			  {
				String agentIP = cb.getParam("dst_ip");
				if(!PluginEngine.isLocal(agentIP)) //ignore local responses 
				{
				boolean addBroker = false;
				String agentPath = cb.getParam("dst_region") + "_" + cb.getParam("dst_agent");
				//System.out.println(getClass().getName() + ">>> canidate boker :" + agentPath + " canidate ip:" + agentIP) ;
	 		      
				BrokeredAgent ba;
				if(PluginEngine.brokeredAgents.containsKey(agentPath))
				{
					
					ba = PluginEngine.brokeredAgents.get(agentPath);
					//add ip to possible list
					if(!ba.addressMap.containsKey(agentIP)) 
					{
						ba.addressMap.put(agentIP,BrokerStatusType.INIT);
					}
					//reset status if needed
					if((ba.brokerStatus.equals(BrokerStatusType.FAILED) || (ba.brokerStatus.equals(BrokerStatusType.STOPPED))))
					{
							ba.activeAddress = agentIP;
							ba.brokerStatus = BrokerStatusType.INIT;
							addBroker = true;
					}
					
				}
				else
				{
					ba = new BrokeredAgent(agentIP,agentPath);
					PluginEngine.brokeredAgents.put(agentPath, ba);
					addBroker = true;
				}
				//try and connect
				if(addBroker)
				{
					addBroker(agentPath);
				}
			  }
		  		}
			  else
			  {
				  Thread.sleep(1000);
			  }
			  
		  } 
		  catch (Exception ex) 
		  {
			  System.out.println("DiscoveryEngineIPv6 : Run Error " + ex.toString());
		  }
	    }
	  }
	 	  
}