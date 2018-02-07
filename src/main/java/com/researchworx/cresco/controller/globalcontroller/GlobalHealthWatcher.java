package com.researchworx.cresco.controller.globalcontroller;

import com.researchworx.cresco.controller.app.gPayload;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.db.NodeStatusType;
import com.researchworx.cresco.controller.globalscheduler.AppSchedulerEngine;
import com.researchworx.cresco.controller.globalscheduler.ResourceSchedulerEngine;
import com.researchworx.cresco.controller.netdiscovery.*;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class GlobalHealthWatcher implements Runnable {
	private Launcher plugin;
	private CLogger logger;
	private Map<String,String> global_host_map;
    private Timer regionalUpdateTimer;
    private Long gCheckInterval;


    public Boolean SchedulerActive;
    public Boolean AppSchedulerActive;

    public GlobalHealthWatcher(Launcher plugin) {
		this.logger = new CLogger(GlobalHealthWatcher.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
        global_host_map = new HashMap<>();
        regionalUpdateTimer = new Timer();
        regionalUpdateTimer.scheduleAtFixedRate(new GlobalHealthWatcher.GlobalNodeStatusWatchDog(plugin, logger), 500, 15000);//remote
        gCheckInterval = plugin.getConfig().getLongParam("watchdogtimer",5000L);
        SchedulerActive = false;
        AppSchedulerActive = false;
        plugin.setResourceScheduleQueue(new LinkedBlockingQueue<MsgEvent>());
    }

	public void shutdown() {

        if(plugin.cstate.isRegionalController()) {
            MsgEvent le = new MsgEvent(MsgEvent.Type.CONFIG, plugin.cstate.getGlobalRegion(), plugin.cstate.getGlobalAgent(), plugin.cstate.getControllerId(), "disabled");

            le.setParam("src_region", plugin.getRegion());
            le.setParam("src_agent", plugin.getAgent());
            le.setParam("src_plugin", plugin.cstate.getControllerId());

            le.setParam("dst_region",plugin.cstate.getGlobalRegion());
            le.setParam("dst_agent",plugin.cstate.getGlobalAgent());
            le.setParam("dst_plugin",plugin.cstate.getControllerId());

            le.setParam("is_regional", Boolean.TRUE.toString());
            le.setParam("is_global", Boolean.TRUE.toString());

            le.setParam("region_name",plugin.cstate.getRegionalRegion());


            le.setParam("action", "region_disable");

            //TODO does this need to be added?
            //le.setParam("globalcmd", Boolean.TRUE.toString());

            le.setParam("watchdogtimer", String.valueOf(plugin.getConfig().getLongParam("watchdogtimer",5000L)));
            //MsgEvent re = new RPCCall().call(le);
            MsgEvent re = plugin.getRPC().call(le);
            logger.info("RPC DISABLE: " + re.getMsgBody() + " [" + re.getParams().toString() + "]");

        }
	}

	public void run() {
		try {

            while(!plugin.hasActiveProducter()) {
                logger.trace("GlobalHealthWatcher waiting on Active Producer.");
                Thread.sleep(2500);
            }

            logger.trace("Initial gCheck");

            gCheck(); //do initial check
            this.plugin.setGlobalControllerManagerActive(true);
            logger.trace("GlobalControllerManager is Active");

            while (this.plugin.isActiveBrokerManagerActive()) {
                Thread.sleep(gCheckInterval);
                gCheck();
                gNotify();
			}
		} catch(Exception ex) {
			logger.error("globalwatcher run() " + ex.getMessage());
            logger.error(ex.getStackTrace().toString());
		}
	}

	private void gNotify() {
        logger.trace("gNotify Start");
        try {
            //if there is a remote global controller and it is reachable
            if(plugin.cstate.isRegionalController()) {
                logger.debug("gNotify !Global Controller Message");
                //is the global controller reachable
                if(plugin.isReachableAgent(plugin.cstate.getGlobalControllerPath())) {
                    MsgEvent tick = new MsgEvent(MsgEvent.Type.WATCHDOG, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "WatchDog timer tick. 0");
                    tick.setParam("src_region", this.plugin.getRegion());
                    tick.setParam("src_agent", this.plugin.getAgent());
                    tick.setParam("src_plugin", this.plugin.cstate.getControllerId());

                    tick.setParam("dst_region",plugin.cstate.getGlobalRegion());
                    tick.setParam("dst_agent",plugin.cstate.getGlobalAgent());
                    tick.setParam("dst_plugin",plugin.cstate.getControllerId());

                    tick.setParam("region_name",plugin.cstate.getRegionalRegion());


                    tick.setParam("is_regional", Boolean.TRUE.toString());
                    tick.setParam("is_global", Boolean.TRUE.toString());

                    tick.setParam("watchdog_ts", String.valueOf(System.currentTimeMillis()));
                    tick.setParam("watchdogtimer", String.valueOf(gCheckInterval));

                    logger.trace("gNotify !Global Controller Message : " + tick.getParams().toString());

                    plugin.msgIn(tick);


                }
            }
            //TODO This might be required

            else if(plugin.cstate.isGlobalController()) {
                //plugin.getGDB().watchDogUpdate()
                logger.trace("gNotify Global Controller Message");

                MsgEvent tick = new MsgEvent(MsgEvent.Type.WATCHDOG, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "WatchDog timer tick. 1");
                tick.setParam("src_region", this.plugin.getRegion());
                tick.setParam("src_agent", this.plugin.getAgent());
                tick.setParam("src_plugin", this.plugin.cstate.getControllerId());

                tick.setParam("dst_region",plugin.cstate.getGlobalRegion());
                tick.setParam("dst_agent",plugin.cstate.getGlobalAgent());
                tick.setParam("dst_plugin",plugin.cstate.getControllerId());

                tick.setParam("is_regional", Boolean.TRUE.toString());
                tick.setParam("is_global", Boolean.TRUE.toString());

                tick.setParam("region_name",plugin.cstate.getRegionalRegion());


                tick.setParam("watchdog_ts", String.valueOf(System.currentTimeMillis()));
                tick.setParam("watchdogtimer", String.valueOf(gCheckInterval));
                //logger.error("CALLING FROM GLOBAL HEALTH: " + tick.getParams().toString());

                logger.trace("gNotify Global Controller Message : " + tick.getParams().toString());

                plugin.getGDB().watchDogUpdate(tick);
            }

            else {
                logger.debug("gNotify : Why and I here!");
            }

        }
        catch(Exception ex) {
            logger.error("gNotify() " + ex.getMessage());
        }
        logger.trace("gNotify End");

    }

    private void gCheck() {
        logger.trace("gCheck Start");

        try{

            //Static Remote Global Controller
            String static_global_controller_host = plugin.getConfig().getStringParam("global_controller_host",null);
            if(static_global_controller_host != null) {
                logger.trace("Starting Static Global Controller Check on " + static_global_controller_host);
                if(global_host_map.containsKey(static_global_controller_host)) {
                    if(plugin.isReachableAgent(global_host_map.get(static_global_controller_host))) {
                        logger.trace("Static Global Controller Check " + static_global_controller_host + " Ok.");
                        return;
                    }
                    else {
                        logger.trace("Static Global Controller Check " + static_global_controller_host + " Failed.");
                        plugin.cstate.setRegionalGlobalFailed("gCheck : Static Global Host :" + static_global_controller_host + " not reachable.");
                        global_host_map.remove(static_global_controller_host);
                    }
                }
                String[] globalController = connectToGlobal(staticGlobalDiscovery());
                if(globalController == null) {
                    logger.error("Failed to connect to Global Controller Host :" + static_global_controller_host);
                    plugin.cstate.setRegionalGlobalFailed("gCheck : Static Global Host :" + static_global_controller_host + " failed to connect.");
                }
                else {
                    plugin.cstate.setRegionalGlobalSuccess(globalController[0],globalController[1], "gCheck : Static Global Host :" + static_global_controller_host + " connected." );
                    logger.trace("Static Global Controller " + static_global_controller_host + " Connect with path: " + plugin.cstate.getGlobalControllerPath());
                    global_host_map.put(static_global_controller_host, plugin.cstate.getGlobalControllerPath());
                    //register with global controller
                    sendGlobalWatchDogRegister();
                    //TODO is this correct?
                    //Don't bother sending on registration, DB is not yet populated, send on second update check
                    //regionalDBexport();
                }
            }
            else if(this.plugin.cstate.isGlobalController()) {
                //Do nothing if already controller, will reinit on regional restart
                logger.trace("Starting Local Global Controller Check");
                if(plugin.getAppScheduleQueue() == null) {
                    plugin.setAppScheduleQueue(new LinkedBlockingQueue<gPayload>());
                    startGlobalSchedulers();
                }
            }
            else {
                logger.trace("Starting Dynamic Global Controller Check");
                //Check if the global controller path exist
                if(plugin.cstate.isGlobalController()) {
                    if (plugin.isReachableAgent(this.plugin.cstate.getGlobalControllerPath())) {
                        logger.debug("Dynamic Global Path : " + this.plugin.cstate.getGlobalControllerPath() + " reachable :" + plugin.isReachableAgent(this.plugin.cstate.getGlobalControllerPath()));
                        return;
                    }
                }
                else {
                    //global controller is not reachable, start dynamic discovery
                    plugin.cstate.setRegionalGlobalFailed("gCheck : Dynamic Global Host :" + this.plugin.cstate.getGlobalControllerPath() + " is not reachable.");
                    List<MsgEvent> discoveryList = dynamicGlobalDiscovery();

                    if(!discoveryList.isEmpty()) {
                        String[] globalController = connectToGlobal(dynamicGlobalDiscovery());
                        if(globalController == null) {
                            logger.error("Failed to connect to Global Controller Host : [unnown]");
                            plugin.cstate.setRegionalGlobalFailed("gCheck : Dynamic Global Host [unknown] failed to connect.");
                        }
                        else {
                            plugin.cstate.setRegionalGlobalSuccess(globalController[0],globalController[1], "gCheck : Dyanmic Global Host :" + globalController[0] + "_" + globalController[1] + " connected." );
                            //register with global controller
                            sendGlobalWatchDogRegister();
                            //TODO is this right to do
                            regionalDBexport();
                        }
                    }
                    else {
                        //No global controller found, starting global services
                        logger.info("No Global Controller Found: Starting Global Services");
                        //start global stuff
                        //create globalscheduler queue
                        //plugin.setResourceScheduleQueue(new LinkedBlockingQueue<MsgEvent>());
                        plugin.setAppScheduleQueue(new LinkedBlockingQueue<gPayload>());
                        startGlobalSchedulers();
                        //end global start
                        this.plugin.cstate.setGlobalSuccess("gCheck : Creating Global Host");
                        logger.info("Global: " + this.plugin.cstate.getRegionalRegion() + " Agent: " + this.plugin.cstate.getRegionalAgent());

                        sendGlobalWatchDogRegister();
                    }
                }
            }

        }
        catch(Exception ex) {
            logger.error("gCheck() " +ex.getMessage());
        }
        logger.trace("gCheck End");

    }

    private Boolean startGlobalSchedulers() {
        boolean isStarted = false;
        try {
            //Start Global Controller Services
            logger.info("Initialized");
            ResourceSchedulerEngine se = new ResourceSchedulerEngine(plugin,this);
            Thread schedulerEngineThread = new Thread(se);
            schedulerEngineThread.start();

            AppSchedulerEngine ae = new AppSchedulerEngine(plugin,this);
            Thread appSchedulerEngineThread = new Thread(ae);
            appSchedulerEngineThread.start();

            isStarted = true;
        }
        catch (Exception ex) {
            logger.error("startGlobalSchedulers() " + ex.getMessage());
        }
        return isStarted;
    }

    private String[] connectToGlobal(List<MsgEvent> discoveryList) {
        String[] globalController = null;
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
	            if((cme.getParam("dst_region") != null) && (cme.getParam("dst_agent")) !=null) {
                    String cGlobalPath = cme.getParam("dst_region") + "_" + (cme.getParam("dst_agent"));
                    if(!this.plugin.isReachableAgent(cGlobalPath)) {
                        plugin.getIncomingCanidateBrokers().add(cme);
                        //while
                        int timeout = 0;
                        while((!this.plugin.isReachableAgent(cGlobalPath)) && (timeout < 10)) {
                            logger.trace("Trying to connect to Global Controller : " + cGlobalPath);
                            timeout++;
                            Thread.sleep(1000);
                        }
                        if(this.plugin.isReachableAgent(cGlobalPath)) {
                            globalController = new String[2];
                            globalController[0] = cme.getParam("dst_region");
                            globalController[1] = cme.getParam("dst_agent");
                        }
                    }
                    else {
                        globalController = new String[2];
                        globalController[0] = cme.getParam("dst_region");
                        globalController[1] = cme.getParam("dst_agent");
                    }
                }

            }

        }
        catch(Exception ex) {
            logger.error("connectToGlobal()" + ex.getMessage());
        }

        return globalController;
    }

    private void sendGlobalWatchDogRegister() {

	    try {
            //if(!plugin.cstate.isGlobalController()) {
                //is the global controller reachable
                if(plugin.isReachableAgent(plugin.cstate.getGlobalControllerPath())) {
                    MsgEvent le = new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "enabled");
                    le.setParam("src_region", plugin.getRegion());
                    le.setParam("src_agent", plugin.getAgent());
                    le.setParam("src_plugin", plugin.cstate.getControllerId());

                    le.setParam("dst_region",plugin.cstate.getGlobalRegion());
                    le.setParam("dst_agent",plugin.cstate.getGlobalAgent());
                    le.setParam("dst_plugin",plugin.cstate.getControllerId());

                    le.setParam("is_regional", Boolean.TRUE.toString());
                    le.setParam("is_global", Boolean.TRUE.toString());

                    le.setParam("region_name",plugin.cstate.getRegionalRegion());

                    //le.setParam("dst_region", gPath[0]);
                    le.setParam("is_active", Boolean.TRUE.toString());
                    le.setParam("action", "region_enable");
                    //le.setParam("globalcmd", Boolean.TRUE.toString());
                    le.setParam("watchdogtimer", String.valueOf(plugin.getConfig().getLongParam("watchdogtimer", 5000L)));
                    //this should be RPC, but routing needs to be fixed route 16 -> 32 -> regionsend -> 16 -> 32 -> regionsend (goes to region, not rpc)
                    le.setParam("source","sendGlobalWatchDogRegister()");
                    plugin.msgIn(le);
                }
            //}
        }
        catch(Exception ex) {
	        logger.error("sendGlobalWatchDogRegister() " + ex.getMessage());
        }

    }

    private List<MsgEvent> dynamicGlobalDiscovery() {
        List<MsgEvent> discoveryList = null;
        try {
            discoveryList = new ArrayList<>();

            if (plugin.isIPv6()) {
                DiscoveryClientIPv6 dcv6 = new DiscoveryClientIPv6(plugin);
                discoveryList = dcv6.getDiscoveryResponse(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_ipv6_global_timeout", 2000));
            }
            DiscoveryClientIPv4 dcv4 = new DiscoveryClientIPv4(plugin);
            discoveryList.addAll(dcv4.getDiscoveryResponse(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_ipv4_global_timeout", 2000)));

            if (!discoveryList.isEmpty()) {
                for (MsgEvent ime : discoveryList) {
                    logger.debug("Global Controller Found: " + ime.getParams());
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
                logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("global_controller_host",null));
                TCPDiscoveryStatic ds = new TCPDiscoveryStatic(plugin);
                discoveryList.addAll(ds.discover(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), plugin.getConfig().getStringParam("global_controller_host",null)));
                logger.debug("Static Agent Connection count = {}" + discoveryList.size());
                if (discoveryList.size() == 0) {
                    logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("global_controller_host",null) + " failed! - Restarting Global Discovery");
                } else {
                    //plugin.getIncomingCanidateBrokers().add(discoveryList.get(0)); //perhaps better way to do this
                    logger.info("Global Controller Found: Region: " + discoveryList.get(0).getParam("src_region") + " agent:" + discoveryList.get(0).getParam("src_agent"));
                }
        }
        catch(Exception ex) {
            logger.error("staticGlobalDiscovery() " + ex.getMessage());
        }
        return discoveryList;
    }

    public MsgEvent regionalDBexport() {
        Thread.dumpStack();
        MsgEvent me = null;
	    try {
            if(!this.plugin.cstate.isGlobalController()) {
                //TODO Enable Export
                String dbexport = plugin.getGDB().gdb.getDBExport();

                logger.error("DBEXPORT SIZE: " + dbexport.length());

                //logger.trace("EXPORT" + dbexport + "EXPORT");


                me = new MsgEvent(MsgEvent.Type.CONFIG, this.plugin.getRegion(), this.plugin.getAgent(), this.plugin.getPluginID(), "regionalimport");
                    me.setParam("action", "regionalimport");
                    me.setParam("src_region", this.plugin.getRegion());
                    me.setParam("src_agent", this.plugin.getAgent());
                    me.setParam("src_plugin", this.plugin.cstate.getControllerId());

                    me.setParam("dst_region",plugin.cstate.getGlobalRegion());
                    me.setParam("dst_agent",plugin.cstate.getGlobalAgent());
                    me.setParam("dst_plugin",plugin.cstate.getControllerId());

                    me.setParam("is_regional", Boolean.TRUE.toString());
                    me.setParam("is_global", Boolean.TRUE.toString());

                    me.setParam("exportdata",dbexport);

                    Thread.dumpStack();
                    //logger.error("*" + me.getCompressedParam("exportdata")  + "*");

                    this.plugin.msgIn(me);
                }


        }
        catch(Exception ex) {
            logger.error("regionalDBexport() " + ex.getMessage());
        }
        return me;
    }

    class GlobalNodeStatusWatchDog extends TimerTask {
        private CLogger logger;
        private Launcher plugin;
        public GlobalNodeStatusWatchDog(Launcher plugin, CLogger logger) {
            this.plugin = plugin;
            this.logger = logger;

        }

        public void run() {
            if(plugin.cstate.isGlobalController()) { //only run if node is a global controller
                logger.debug("GlobalNodeStatusWatchDog");
                //update own database

                /*
                Map<String, NodeStatusType> edgeHealthListStatus = plugin.getGDB().getNodeStatus(null, null, null);
                for (Map.Entry<String, NodeStatusType> entry : edgeHealthListStatus.entrySet()) {
                    logger.info("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                }

                Map<String, NodeStatusType> nodeListStatus = plugin.getGDB().getNodeStatus(null, null, null);


                for (Map.Entry<String, NodeStatusType> entry : nodeListStatus.entrySet()) {
*/
                Map<String, NodeStatusType> edgeHealthListStatus = plugin.getGDB().getEdgeHealthStatus(null, null, null);

                for (Map.Entry<String, NodeStatusType> entry : edgeHealthListStatus.entrySet()) {

                logger.debug("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());

                    if(entry.getValue() == NodeStatusType.STALE) { //will include more items once nodes update correctly
                        logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                        //mark node disabled
                        plugin.getGDB().gdb.setNodeParam(entry.getKey(),"is_active",Boolean.FALSE.toString());
                    }
                    else if(entry.getValue() == NodeStatusType.LOST) { //will include more items once nodes update correctly
                        logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                        //remove nodes
                        Map<String,String> nodeParams = plugin.getGDB().gdb.getNodeParams(entry.getKey());
                        String region = nodeParams.get("region");
                        String agent = nodeParams.get("agent");
                        String pluginId = nodeParams.get("plugin");
                        logger.error("Removing " + region + " " + agent + " " + pluginId);
                        plugin.getGDB().removeNode(region,agent,pluginId);
                    }
                    else if(entry.getValue() == NodeStatusType.ERROR) { //will include more items once nodes update correctly
                        logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());

                    } /*else {
                        logger.info("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());
                        Map<String,String> nodeMap = plugin.getGDB().gdb.getNodeParams(entry.getKey());
                        logger.info("Region : " + nodeMap.get("region_name") + " Agent : " + nodeMap.get("agent_name"));
                    }
                    */

                }
            }
            else {
                //send full export of region to global controller
                //Need to develop a better inconsistency method
                //logger.info("Regional Export");
                //TODO enable global export
                //regionalDBexport();

            }
        }
    }
}