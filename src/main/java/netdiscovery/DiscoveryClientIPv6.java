package netdiscovery;

import java.net.Inet6Address;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import plugincore.PluginEngine;
import shared.MsgEvent;

public class DiscoveryClientIPv6 
{
	private static final Logger logger = LoggerFactory.getLogger(DiscoveryClientIPv6.class);
	//private int discoveryTimeout;
	public DiscoveryClientIPv6() {
		//discoveryTimeout = Integer.parseInt(PluginEngine.config.getParam("discoverytimeout")); 
		//System.out.println("DiscoveryClient : discoveryTimeout = " + discoveryTimeout);
		//discoveryTimeout = 1000;
	}
	
	public  List<MsgEvent> getDiscoveryResponse(DiscoveryType disType, int discoveryTimeout) {
			List<MsgEvent> discoveryList = new ArrayList<MsgEvent>();
		try {
			
			
			while(PluginEngine.clientDiscoveryActiveIPv6) {
				logger.debug("Discovery already underway, waiting..");
				Thread.sleep(2500);
			}
			PluginEngine.clientDiscoveryActiveIPv6 = true;
			//Searching local network [ff02::1:c]
			String multiCastNetwork = "ff02::1:c";
			DiscoveryClientWorkerIPv6 dcw = new DiscoveryClientWorkerIPv6(disType, discoveryTimeout,multiCastNetwork);
			//populate map with possible peers
			logger.debug("Searching {}", multiCastNetwork);
			discoveryList.addAll(dcw.discover());
			
			//limit discovery for the moment
			//Searching site network [ff05::1:c]
			//multiCastNetwork = "ff05::1:c";
			//dcw = new DiscoveryClientWorkerIPv6(discoveryTimeout,multiCastNetwork);
			//System.out.println("DiscoveryClientIPv6 : searching " + multiCastNetwork);
			//dcw.discover();
		} catch(Exception ex) {
			logger.error("getDiscoveryMap {}", ex.getMessage());
			
		}
		PluginEngine.clientDiscoveryActiveIPv6 = false;
		return discoveryList;
	}
	
	public boolean isReachable(String hostname) {
		boolean reachable = false;
		try {
		   //also, this fails for an invalid address, like "www.sjdosgoogle.com1234sd" 
	       //InetAddress[] addresses = InetAddress.getAllByName("www.google.com");
			Inet6Address address =  (Inet6Address) Inet6Address.getByName(hostname);
	        if (address.isReachable(10000)) {
	        	reachable = true;
	        } else {
	           reachable = false;
	        }
		} catch(Exception ex) {
			logger.error("isReachable {}", ex.getMessage());
		}
		return reachable;
	}
	


}
