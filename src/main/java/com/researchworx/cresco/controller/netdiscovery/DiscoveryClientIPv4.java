package com.researchworx.cresco.controller.netdiscovery;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class DiscoveryClientIPv4 {
    //private int discoveryTimeout;
    private Launcher plugin;
    private CLogger logger;

    public DiscoveryClientIPv4(Launcher plugin) {
        this.logger = new CLogger(DiscoveryClientIPv4.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(),CLogger.Level.Info);
        this.plugin = plugin;
        //discoveryTimeout = Integer.parseInt(PluginEngine.config.getParam("discoverytimeout"));
        //System.out.println("DiscoveryClient : discoveryTimeout = " + discoveryTimeout);
        //discoveryTimeout = 1000;
    }

    public List<MsgEvent> getDiscoveryResponse(DiscoveryType disType, int discoveryTimeout) {
        List<MsgEvent> discoveryList = new ArrayList<>();
        try {
            while (this.plugin.isClientDiscoveryActiveIPv4()) {
                logger.debug("Discovery already underway, waiting..");
                Thread.sleep(2500);
            }
            this.plugin.setClientDiscoveryActiveIPv4(true);
            //Searching local network 255.255.255.255
            String broadCastNetwork = "255.255.255.255";

            DiscoveryClientWorkerIPv4 dcw = new DiscoveryClientWorkerIPv4(this.plugin, disType, discoveryTimeout, broadCastNetwork);
            //populate map with possible peers
            logger.debug("Searching {}", broadCastNetwork);
            discoveryList.addAll(dcw.discover());


        } catch (Exception ex) {
            //System.out.println("IPv4 getDiscoveryMap " + ex.getMessage());
            logger.error("getDiscoveryMap {}", ex.getMessage());

        }
        this.plugin.setClientDiscoveryActiveIPv4(false);
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

    public boolean isReachable(String hostname) {
        boolean reachable = false;
        try {
            //also, this fails for an invalid address, like "www.sjdosgoogle.com1234sd"
            //InetAddress[] addresses = InetAddress.getAllByName("www.google.com");
            InetAddress address = InetAddress.getByName(hostname);

            reachable = address.isReachable(10000);

        } catch (Exception ex) {
            System.out.println("DiscoveryClient : isReachable : Error " + ex.toString());
        }
        return reachable;
    }


}
