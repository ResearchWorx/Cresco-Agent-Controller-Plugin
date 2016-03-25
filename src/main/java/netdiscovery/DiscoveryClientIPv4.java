package netdiscovery;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plugincore.PluginEngine;
import shared.MsgEvent;

public class DiscoveryClientIPv4 {
	private static final Logger logger = LoggerFactory.getLogger(DiscoveryClientIPv4.class);
	//private int discoveryTimeout;

	public DiscoveryClientIPv4()
	{
		//discoveryTimeout = Integer.parseInt(PluginEngine.config.getParam("discoverytimeout")); 
		//System.out.println("DiscoveryClient : discoveryTimeout = " + discoveryTimeout);
		//discoveryTimeout = 1000;
	}

	public List<MsgEvent> getDiscoveryResponse(DiscoveryType disType, int discoveryTimeout) {
		List<MsgEvent> discoveryList = new ArrayList<>();
		try {


			while(PluginEngine.clientDiscoveryActiveIPv4) {
				logger.debug("Discovery already underway, waiting..");
				Thread.sleep(2500);
			}
			PluginEngine.clientDiscoveryActiveIPv4 = true;
			//Searching local network 255.255.255.255
			String broadCastNetwork = "255.255.255.255";
			DiscoveryClientWorkerIPv4 dcw = new DiscoveryClientWorkerIPv4(disType, discoveryTimeout, broadCastNetwork);
			//populate map with possible peers
			logger.debug("Searching {}", broadCastNetwork);
			discoveryList.addAll(dcw.discover());
		} catch(Exception ex) {
			logger.error("getDiscoveryMap {}", ex.getMessage());

		}
		PluginEngine.clientDiscoveryActiveIPv4 = false;
		return discoveryList;
	}
	
	/*public void getDiscoveryMap(int discoveryTimeout)
	{
		try
		{
			while(PluginEngine.clientDiscoveryActiveIPv4)
			{
				System.out.println("DiscoveryClient : Discovery already underway : waiting..");
				Thread.sleep(2500);
			}
			PluginEngine.clientDiscoveryActiveIPv4 = true;
			DiscoveryClientWorkerIPv4 dcw = new DiscoveryClientWorkerIPv4(discoveryTimeout);
			dcw.discover();
		}
		catch(Exception ex)
		{
			System.out.println("DiscoveryClient Error : " + ex.toString());
		}
		PluginEngine.clientDiscoveryActiveIPv4 = false;
		
	}*/
	
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
