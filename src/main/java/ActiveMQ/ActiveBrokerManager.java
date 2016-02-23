package ActiveMQ;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.SocketException;

import plugincore.PluginEngine;
import shared.MsgEvent;


public class ActiveBrokerManager implements Runnable 
{
	//private MulticastSocket socket;
	
	public ActiveBrokerManager() throws SocketException
	{
	
	}
	  
	public void shutdown()
	{
	
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
				System.out.println("Found canidate broker: IP=" + cb.getParam("dst_ip") + " Region=" + cb.getParam("dst_region") + " Agent=" + cb.getParam("dst_agent"));
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


}