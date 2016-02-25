package ActiveMQ;

import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import plugincore.PluginEngine;
import shared.MsgEvent;


public class ActiveBrokerManager implements Runnable 
{
	//private MulticastSocket socket;
	private Timer timer;
	public ActiveBrokerManager()
	{
		timer = new Timer();
	    //timer.scheduleAtFixedRate(new BrokerWatchDog(), 500, 300000);//remote 
		timer.scheduleAtFixedRate(new BrokerWatchDog(), 500, 15000);//remote 
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
							//System.out.println("BA EXIST ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
					}
					
					//System.out.println("BA EXIST ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
					
				}
				else
				{
					ba = new BrokeredAgent(agentIP,agentPath);
					PluginEngine.brokeredAgents.put(agentPath, ba);
					addBroker = true;
					//System.out.println("BA NEW ADDING agentPath: " + agentPath + " remote_ip: " + agentIP);
				}
				//try and connect
				if(addBroker)
				{
					addBroker(agentPath);
				}
			  }
		  		//Thread.sleep(500); //allow HM to catch up
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