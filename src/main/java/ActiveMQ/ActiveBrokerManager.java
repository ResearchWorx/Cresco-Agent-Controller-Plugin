package ActiveMQ;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;

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
			
		  System.out.println("Adding Broker: " + agentPath + " IP:" + ba.activeAddress);
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
				String agentPath = cb.getParam("dst_region") + cb.getParam("dst_agent");
				String agentIP = cb.getParam("dst_ip");
				BrokeredAgent ba;
				if(PluginEngine.brokeredAgents.containsKey(agentPath))
				{
					ba = PluginEngine.brokeredAgents.get(agentPath);
					if((ba.brokerStatus.equals(BrokerStatusType.FAILED) || (ba.brokerStatus.equals(BrokerStatusType.STOPPED))))
					{
						ba.addressMap.put(agentIP,BrokerStatusType.INIT);
					}
				}
				else
				{
					ba = new BrokeredAgent(agentIP,agentPath);
					PluginEngine.brokeredAgents.put(agentPath, ba);
				}
				//try and connect
				
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
	  /*
	    public static boolean processPeer(String peer,String agentpath)
	    {
	    	boolean isPeer = false;
	    	boolean isIPv6 = false;
	    	try
	    	{
	    		if((!PluginEngine.localAddresses().contains(peer) && (!PluginEngine.abhm.containsKey(peer))) && (!PluginEngine.pbhm.containsKey(agentpath)))
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
	    				isReachable = PluginEngine.dcv6.isReachable(peer);
	    				isIPv6 = true;
	    			}
	    			else if(peerAddress instanceof Inet4Address)
	    			{
	    				isReachable = PluginEngine.dc.isReachable(peer);
	    			}
	    		
	    			if(isReachable)
	    			{
	    				System.out.println("ProcessPeer: " + peer + " is reachable");
	    				System.out.println("adding network connect for peer: " + peer);
	    				if(isIPv6)
	    				{
	    					PluginEngine.broker.AddNetworkConnector("[" + peer + "]");
	        			}
	    				else
	    				{
	    					PluginEngine.broker.AddNetworkConnector(peer);
	        			}
	    				//peerList.add(peer);
	    				PluginEngine.abhm.put(peer,agentpath);
	    				PluginEngine.pbhm.put(agentpath,peer);
	    				
	    				isPeer = true;
	    			}
	    		}
	    	}
	    	catch(Exception ex)
	    	{
	    		System.out.println("PluginEngine : Process Peer " + ex.toString());
	    		
	    	}
	    	return isPeer;
	    }
*/

}