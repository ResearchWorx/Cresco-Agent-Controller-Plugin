package netdiscovery;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import plugincore.PluginEngine;
import shared.MsgEvent;

public class DiscoveryClientIPv4 
{
	//private int discoveryTimeout;
	public DiscoveryClientIPv4()
	{
		//discoveryTimeout = Integer.parseInt(PluginEngine.config.getParam("discoverytimeout")); 
		//System.out.println("DiscoveryClient : discoveryTimeout = " + discoveryTimeout);
		//discoveryTimeout = 1000;
	}
	
	public List<MsgEvent> getDiscoveryMap(int discoveryTimeout)
	{
		//Map<String,String> disMap = null;
		List<MsgEvent> disList = null;
		try
		{
			while(PluginEngine.clientDiscoveryActive)
			{
				System.out.println("DiscoveryClient : Discovery already underway : waiting..");
				Thread.sleep(2500);
			}
			PluginEngine.clientDiscoveryActive = true;
			DiscoveryClientWorkerIPv4 dcw = new DiscoveryClientWorkerIPv4(discoveryTimeout);
			disList = dcw.discover();
		}
		catch(Exception ex)
		{
			System.out.println("DiscoveryClient Error : " + ex.toString());
		}
		PluginEngine.clientDiscoveryActive = false;
		
		return disList;
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
