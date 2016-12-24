package com.researchworx.cresco.controller.globalcontroller;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.netdiscovery.DiscoveryClientIPv4;
import com.researchworx.cresco.controller.netdiscovery.DiscoveryClientIPv6;
import com.researchworx.cresco.controller.netdiscovery.DiscoveryStatic;
import com.researchworx.cresco.controller.netdiscovery.DiscoveryType;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GlobalControllerMonitor implements Runnable {
	private Launcher plugin;
	private CLogger logger;
	private DiscoveryClientIPv4 dcv4;
	private DiscoveryClientIPv6 dcv6;
	private Map<String,String> global_host_map;

	public GlobalControllerMonitor(Launcher plugin, DiscoveryClientIPv4 dcv4, DiscoveryClientIPv6 dcv6) {
		this.logger = new CLogger(GlobalControllerMonitor.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
		this.plugin = plugin;
        this.dcv4 = dcv4;
        this.dcv6 = dcv6;
        global_host_map = new HashMap<>();
	}

	public void shutdown() {


	}

	public void run() {
		try {
            logger.trace("Initial gCheck");
		    gCheck(); //do initial check
            this.plugin.setGlobalControllerManagerActive(true);
            logger.trace("GlobalControllerManager is Active");

            while (this.plugin.isActiveBrokerManagerActive()) {
                Thread.sleep(5000);
                logger.trace("Loop gCheck");
                gCheck();
			}
		} catch(Exception ex) {
			logger.error("Run {}", ex.getMessage());
            logger.error(ex.getStackTrace().toString());
		}
	}

	private void gCheck() {
	    try{
            //Static Remote Global Controller
	        String static_global_controller_host = plugin.getConfig().getStringParam("global_controller_host");
	        if(static_global_controller_host != null) {
                this.plugin.isGlobalController = false;
                logger.trace("Starting Static Global Controller Check");
	            if(global_host_map.containsKey(static_global_controller_host)) {
                    if(plugin.isReachableAgent(global_host_map.get(static_global_controller_host))) {
                        return;
                    }
                    else {
                        plugin.globalControllerPath = null;
                        global_host_map.remove(static_global_controller_host);
                    }
                }
                String globalPath = connectToGlobal(staticGlobalDiscovery());
                if(globalPath == null) {
                    logger.error("Failed to connect to Global Controller Host :" + static_global_controller_host);
                    this.plugin.globalControllerPath = null;
                }
                else {
                    global_host_map.put(static_global_controller_host, globalPath);
                    plugin.globalControllerPath = globalPath;
                }
            }
            else if(this.plugin.isGlobalController) {
                //Do nothing if already controller, will reinit on regional restart
                logger.trace("Starting Local Global Controller Check");
            }
            else {
                logger.trace("Starting Dynamic Global Controller Check");
                //Check if the global controller path exist
                if(plugin.isReachableAgent(this.plugin.globalControllerPath)) {
                   return;
                }
                else {
                    //global controller is not reachable, start dynamic discovery
                    this.plugin.isGlobalController = false;
                    this.plugin.globalControllerPath = null;
                    List<MsgEvent> discoveryList = dynamicGlobalDiscovery();

                    if(!discoveryList.isEmpty()) {
                        String globalPath = connectToGlobal(dynamicGlobalDiscovery());
                        if(globalPath == null) {
                            logger.error("Failed to connect to Global Controller Host :" + globalPath);
                        }
                        else {
                            plugin.globalControllerPath = globalPath;
                        }
                    }
                    else {
                        //No global controller found, starting global services
                        logger.info("No Global Controller Found: Starting Global Services");
                        //start global stuff
                        this.plugin.isGlobalController = true;
                    }
                }
            }
        }
        catch(Exception ex) {
	        logger.error(ex.getMessage());
        }
	}


    private String connectToGlobal(List<MsgEvent> discoveryList) {
        String globalPath = null;
        MsgEvent cme = null;
        int cme_count = 0;

	    try {
	        for(MsgEvent ime : discoveryList) {
	            logger.trace("Global Discovery Response : " + ime.getParams().toString());
	            //determine least loaded
	            String ime_count_string = ime.getParam("agent_count");
	            if(ime_count_string != null) {
                    int ime_count = Integer.parseInt(ime_count_string);
	                if(cme == null) {
                        cme = ime;
                        cme_count = ime_count;
                    }
                    else {
                        if(ime_count < cme_count) {
                            cme = ime;
                            cme_count = ime_count;
                        }
                    }
                }

            }
            //if we have a canadate, check to see if we are already connected a regions
            if(cme != null) {
	            if((cme.getParam("src_region") != null) && (cme.getParam("src_agent")) !=null) {
                    String cGlobalPath = cme.getParam("src_region") + "_" + (cme.getParam("src_agent"));
                    if(!this.plugin.isReachableAgent(cGlobalPath)) {
                        plugin.getIncomingCanidateBrokers().offer(cme);
                        //while
                        int timeout = 0;
                        while((!this.plugin.isReachableAgent(cGlobalPath)) && (timeout < 10)) {
                            logger.trace("Trying to connect to Global Controller : " + cGlobalPath);
                            timeout++;
                            Thread.sleep(1000);
                        }
                        if(this.plugin.isReachableAgent(cGlobalPath)) {
                            globalPath = cGlobalPath;
                        }
                    }
                    else {
                        globalPath = cGlobalPath;
                    }
                }

            }


        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return globalPath;
    }


    private List<MsgEvent> dynamicGlobalDiscovery() {
        List<MsgEvent> discoveryList = null;
        try {
            discoveryList = new ArrayList<>();
            if (plugin.isIPv6()) {
                discoveryList = dcv6.getDiscoveryResponse(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_ipv6_global_timeout", 2000));
            }
            discoveryList.addAll(dcv4.getDiscoveryResponse(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_ipv4_global_timeout", 2000)));

            if (!discoveryList.isEmpty()) {
                for (MsgEvent ime : discoveryList) {
                    logger.info("Global Controller Found: " + ime.getParams());
                }
            }
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return discoveryList;
    }

    private List<MsgEvent> staticGlobalDiscovery() {
        List<MsgEvent> discoveryList = null;
        try {
            discoveryList = new ArrayList<>();
                logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("global_controller_host"));
                DiscoveryStatic ds = new DiscoveryStatic(plugin);
                discoveryList.addAll(ds.discover(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), plugin.getConfig().getStringParam("global_controller_host")));
                logger.debug("Static Agent Connection count = {}" + discoveryList.size());
                if (discoveryList.size() == 0) {
                    logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("global_controller_host") + " failed! - Restarting Global Discovery");
                } else {
                    //plugin.getIncomingCanidateBrokers().offer(discoveryList.get(0)); //perhaps better way to do this
                    logger.info("Global Controller Found: " + discoveryList.get(0).getParams());
                }
        }
        catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return discoveryList;
    }

    /*
	private void globalDiscovery() {
        List<MsgEvent> discoveryList = new ArrayList<>();
	    //Either start global controller services, statically contact a global controller, or dynamically discover controller
        //Try and discover global controller, connect to only one controller
        //static discovery
        if(plugin.getConfig().getStringParam("global_controller_host") != null) {
            //do directed discovery
            while(discoveryList.size() == 0) {
                logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("global_controller_host"));
                DiscoveryStatic ds = new DiscoveryStatic(plugin);
                discoveryList.addAll(ds.discover(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout",10000), plugin.getConfig().getStringParam("global_controller_host")));
                logger.debug("Static Agent Connection count = {}" + discoveryList.size());
                if(discoveryList.size() == 0) {
                    logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("global_controller_host") + " failed! - Restarting Global Discovery");
                }
                else {
                    //plugin.getIncomingCanidateBrokers().offer(discoveryList.get(0)); //perhaps better way to do this
                    logger.info("Global Controller Found: " + discoveryList.get(0).getParams());
                }
            }
            //dynamic discovery of global controller
        } else {

            if (plugin.isIPv6()) {
                discoveryList = dcv6.getDiscoveryResponse(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_ipv6_global_timeout", 2000));
            }
            discoveryList.addAll(dcv4.getDiscoveryResponse(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_ipv4_global_timeout", 2000)));

            if (!discoveryList.isEmpty()) {
                for (MsgEvent ime : discoveryList) {
                    //We only want to connect to a single global controller, so this needs work.
                    //plugin.getIncomingCanidateBrokers().offer(ime);
                    logger.info("Global Controller Found: " + ime.getParams());
                }
            }
            else {//No global controller found, starting global services
                logger.info("No Global Controller Found: Starting Global Services");
                //start global stuff
            }
        }

        //end
    }

*/
}