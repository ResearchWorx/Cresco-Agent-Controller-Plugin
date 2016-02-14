package netdiscovery;

import java.net.InetAddress;
import java.util.Map;

import plugincore.PluginEngine;

public class DiscoveryClient 
{
	private int discoveryTimeout;
	public DiscoveryClient()
	{
		//discoveryTimeout = Integer.parseInt(PluginEngine.config.getParam("discoverytimeout")); 
		//System.out.println("DiscoveryClient : discoveryTimeout = " + discoveryTimeout);
		discoveryTimeout = 1000;
	}
	
	public Map<String,String> getDiscoveryMap()
	{
		Map<String,String> disMap = null;
		
		try
		{
			while(PluginEngine.clientDiscoveryActive)
			{
				System.out.println("DiscoveryClient : Discovery already underway : waiting..");
				Thread.sleep(2500);
			}
			PluginEngine.clientDiscoveryActive = true;
			DiscoveryClientWorker dcw = new DiscoveryClientWorker(discoveryTimeout);
			disMap = dcw.getDiscoveryMap();
		}
		catch(Exception ex)
		{
			System.out.println("DiscoveryClient Error : " + ex.toString());
		}
		PluginEngine.clientDiscoveryActive = false;
		
		return disMap;
	}
	
	public boolean isReachable(String hostname)
	{
		boolean reachable = false;
		try
		{
		   //also, this fails for an invalid address, like "www.sjdosgoogle.com1234sd" 
	       //InetAddress[] addresses = InetAddress.getAllByName("www.google.com");
			InetAddress address =  InetAddress.getByName(hostname);
	      
	        if (address.isReachable(10000))
	        {   
	        	reachable = true;
	        }
	        else
	        {
	           reachable = false;
	        }
	      
		}
		catch(Exception ex)
		{
			System.out.println("DiscoveryClient : isReachable : Error " + ex.toString());
		}
		return reachable;
	}
	


}
