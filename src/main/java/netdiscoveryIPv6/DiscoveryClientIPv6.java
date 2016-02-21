package netdiscoveryIPv6;

import java.net.Inet6Address;
import java.util.Map;

import plugincore.PluginEngine;

public class DiscoveryClientIPv6 
{
	//private int discoveryTimeout;
	public DiscoveryClientIPv6()
	{
		//discoveryTimeout = Integer.parseInt(PluginEngine.config.getParam("discoverytimeout")); 
		//System.out.println("DiscoveryClient : discoveryTimeout = " + discoveryTimeout);
		//discoveryTimeout = 1000;
	}
	
	public Map<String,String> getDiscoveryMap(int discoveryTimeout)
	{
		Map<String,String> disMap = null;
		
		try
		{
			while(PluginEngine.clientDiscoveryActiveIPv6)
			{
				System.out.println("DiscoveryClientIPv6 : Discovery already underway : waiting..");
				Thread.sleep(2500);
			}
			PluginEngine.clientDiscoveryActiveIPv6 = true;
			//Searching local network [ff02::1:c]
			String multiCastNetwork = "ff02::1:c";
			DiscoveryClientWorkerIPv6 dcw = new DiscoveryClientWorkerIPv6(discoveryTimeout,multiCastNetwork);
			//populate map with possible peers
			disMap = dcw.getDiscoveryMap();
			if(disMap.isEmpty())
			{
				System.out.println("DiscoveryClientIPv6 : No local hosts found - searching site..");
				//Searching site network [ff05::1:c]
				multiCastNetwork = "ff05::1:c";
				dcw = new DiscoveryClientWorkerIPv6(discoveryTimeout,multiCastNetwork);
				disMap = dcw.getDiscoveryMap();
			}
		}
		catch(Exception ex)
		{
			System.out.println("DiscoveryClientIPv6 Error : " + ex.toString());
		}
		PluginEngine.clientDiscoveryActiveIPv6 = false;
		
		return disMap;
	}
	
	public boolean isReachable(String hostname)
	{
		boolean reachable = false;
		try
		{
		   //also, this fails for an invalid address, like "www.sjdosgoogle.com1234sd" 
	       //InetAddress[] addresses = InetAddress.getAllByName("www.google.com");
			Inet6Address address =  (Inet6Address) Inet6Address.getByName(hostname);
	      
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
			System.out.println("DiscoveryClientIPv6 : isReachable : Error " + ex.toString());
		}
		return reachable;
	}
	


}
