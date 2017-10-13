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

        if(plugin.getGlobalControllerPath() != null) {
            String[] globalPath = plugin.getGlobalControllerPath().split("_");
            MsgEvent le = new MsgEvent(MsgEvent.Type.CONFIG, globalPath[0], globalPath[1], null, "disabled");
            le.setParam("src_region", plugin.getRegion());
            le.setParam("dst_region", globalPath[0]);
            le.setParam("globalcmd", Boolean.TRUE.toString());
            le.setParam("action", "disable");

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
        logger.trace("gNotify End");
        try {
            //if there is a remote global controller and it is reachable
            String globalPath = plugin.getGlobalControllerPath();
            if((globalPath != null) && (!plugin.isGlobalController())) {
                logger.debug("gNotify !Global Controller Message");
                //is the global controller reachable
                if(plugin.isReachableAgent(plugin.getGlobalControllerPath())) {
                    String[] gPath = globalPath.split("_");
                    MsgEvent tick = new MsgEvent(MsgEvent.Type.WATCHDOG, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "WatchDog timer tick.");
                    tick.setParam("globalcmd", Boolean.TRUE.toString());
                    tick.setParam("src_region", plugin.getRegion());
                    tick.setParam("dst_region", gPath[0]);
                    tick.setParam("watchdog_ts", String.valueOf(System.currentTimeMillis()));
                    tick.setParam("watchdogtimer", String.valueOf(gCheckInterval));

                    logger.trace("gNotify !Global Controller Message : " + tick.getParams().toString());

                    plugin.msgIn(tick);


                }
            }
            else if(plugin.isGlobalController()) {
                //plugin.getGDB().watchDogUpdate()
                logger.trace("gNotify Global Controller Message");

                MsgEvent tick = new MsgEvent(MsgEvent.Type.WATCHDOG, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "WatchDog timer tick.");
                tick.setParam("src_region", plugin.getRegion());
                tick.setParam("dst_region", plugin.getRegion());
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
            String static_global_controller_host = plugin.getConfig().getStringParam("gc_host",null);
            if(static_global_controller_host != null) {
                this.plugin.setGlobalController(false);
                logger.trace("Starting Static Global Controller Check");
                if(global_host_map.containsKey(static_global_controller_host)) {
                    if(plugin.isReachableAgent(global_host_map.get(static_global_controller_host))) {
                        return;
                    }
                    else {
                        this.plugin.setGlobalControllerPath(null);
                        global_host_map.remove(static_global_controller_host);
                    }
                }
                String globalPath = connectToGlobal(staticGlobalDiscovery());
                if(globalPath == null) {
                    logger.error("Failed to connect to Global Controller Host :" + static_global_controller_host);
                    this.plugin.setGlobalControllerPath(null);
                }
                else {
                    global_host_map.put(static_global_controller_host, globalPath);
                    plugin.setGlobalControllerPath(globalPath);
                    //register with global controller
                    sendGlobalWatchDogRegister();
                    //todo is this correct?
                    regionalDBexport();
                }
            }
            else if(this.plugin.isGlobalController()) {
                //Do nothing if already controller, will reinit on regional restart
                logger.trace("Starting Local Global Controller Check");
            }
            else {
                logger.trace("Starting Dynamic Global Controller Check");
                //Check if the global controller path exist
                if(plugin.isReachableAgent(this.plugin.getGlobalControllerPath())) {
                    logger.debug("Global Path : " +  this.plugin.getGlobalControllerPath() + " reachable :" + plugin.isReachableAgent(this.plugin.getGlobalControllerPath()));
                    return;
                }
                else {
                    //global controller is not reachable, start dynamic discovery
                    this.plugin.setGlobalController(false);
                    this.plugin.setGlobalControllerPath(null);
                    List<MsgEvent> discoveryList = dynamicGlobalDiscovery();

                    if(!discoveryList.isEmpty()) {
                        String globalPath = connectToGlobal(dynamicGlobalDiscovery());
                        if(globalPath == null) {
                            logger.error("Failed to connect to Global Controller Host :" + globalPath);
                        }
                        else {
                            this.plugin.setGlobalControllerPath(globalPath);
                            //register with global controller
                            sendGlobalWatchDogRegister();
                            //todo is this right to do
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
                        this.plugin.setGlobalController(true);


                    }
                }
            }

        }
        catch(Exception ex) {
            logger.error("gCheck() " +ex.getMessage());
        }
        logger.trace("gCheck End");

    }

    private void gCheck2() {
        logger.trace("gCheck Start");

        try{

            //Static Remote Global Controller
	        String static_global_controller_host = plugin.getConfig().getStringParam("gc_host",null);
            if(static_global_controller_host != null) {
                this.plugin.setGlobalController(false);
                logger.trace("Starting Static Global Controller Check");
	            if(global_host_map.containsKey(static_global_controller_host)) {
                    if(plugin.isReachableAgent(global_host_map.get(static_global_controller_host))) {
                        return;
                    }
                    else {
                        this.plugin.setGlobalControllerPath(null);
                        global_host_map.remove(static_global_controller_host);
                    }
                }
                String globalPath = connectToGlobal(staticGlobalDiscovery());
                if(globalPath == null) {
                    logger.error("Failed to connect to Global Controller Host :" + static_global_controller_host);
                    this.plugin.setGlobalControllerPath(null);
                }
                else {
                    global_host_map.put(static_global_controller_host, globalPath);
                    plugin.setGlobalControllerPath(globalPath);
                    //register with global controller
                    sendGlobalWatchDogRegister();
                    //todo is this correct?
                    regionalDBexport();
                }
            }
            else if(this.plugin.isGlobalController()) {
                //Do nothing if already controller, will reinit on regional restart
                logger.trace("Starting Local Global Controller Check");
            }
            else {
                logger.trace("Starting Dynamic Global Controller Check");
                //Check if the global controller path exist
                if(plugin.isReachableAgent(this.plugin.getGlobalControllerPath())) {
                    logger.debug("Global Path : " +  this.plugin.getGlobalControllerPath() + " reachable :" + plugin.isReachableAgent(this.plugin.getGlobalControllerPath()));
                   return;
                }
                else {
                    //global controller is not reachable, start dynamic discovery
                    this.plugin.setGlobalController(false);
                    this.plugin.setGlobalControllerPath(null);
                    List<MsgEvent> discoveryList = dynamicGlobalDiscovery();

                    if(!discoveryList.isEmpty()) {
                        String globalPath = connectToGlobal(dynamicGlobalDiscovery());
                        if(globalPath == null) {
                            logger.error("Failed to connect to Global Controller Host :" + globalPath);
                        }
                        else {
                            this.plugin.setGlobalControllerPath(globalPath);
                            //register with global controller
                            sendGlobalWatchDogRegister();
                            //todo is this right to do
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
                        this.plugin.setGlobalController(true);


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
            logger.info("Starting Global Scheduler Service");
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
            logger.error("connectToGlobal()" + ex.getMessage());
        }

        return globalPath;
    }

    private void sendGlobalWatchDogRegister() {

	    try {
            String globalPath = plugin.getGlobalControllerPath();
            if((globalPath != null) && (!plugin.isGlobalController())) {
                //is the global controller reachable
                if(plugin.isReachableAgent(plugin.getGlobalControllerPath())) {
                    String[] gPath = globalPath.split("_");
                    MsgEvent le = new MsgEvent(MsgEvent.Type.CONFIG, plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), "enabled");
                    le.setParam("src_region", plugin.getRegion());
                    le.setParam("dst_region", gPath[0]);
                    //le.setParam("is_active", Boolean.TRUE.toString());
                    le.setParam("action", "enable");
                    //le.setParam("globalcmd", Boolean.TRUE.toString());
                    le.setParam("watchdogtimer", String.valueOf(plugin.getConfig().getLongParam("watchdogtimer", 5000L)));
                    //this should be RPC, but routing needs to be fixed route 16 -> 32 -> regionsend -> 16 -> 32 -> regionsend (goes to region, not rpc)
                    plugin.msgIn(le);
                }
            }
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
                logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("gc_host",null));
                TCPDiscoveryStatic ds = new TCPDiscoveryStatic(plugin);
                discoveryList.addAll(ds.discover(DiscoveryType.GLOBAL, plugin.getConfig().getIntegerParam("discovery_static_agent_timeout", 10000), plugin.getConfig().getStringParam("gc_host",null)));
                logger.debug("Static Agent Connection count = {}" + discoveryList.size());
                if (discoveryList.size() == 0) {
                    logger.info("Static Region Connection to Global Controller : " + plugin.getConfig().getStringParam("gc_host",null) + " failed! - Restarting Global Discovery");
                } else {
                    //plugin.getIncomingCanidateBrokers().add(discoveryList.get(0)); //perhaps better way to do this
                    logger.info("Global Controller Found: " + discoveryList.get(0).getParams());
                }
        }
        catch(Exception ex) {
            logger.error("staticGlobalDiscovery() " + ex.getMessage());
        }
        return discoveryList;
    }

    public MsgEvent regionalDBexport() {
        MsgEvent me = null;
	    try {
            if(!this.plugin.isGlobalController()) {
                if(this.plugin.getGlobalControllerPath() != null) {
                    //todo Enable Export
                    String dbexport = plugin.getGDB().gdb.getDBExport();
                    logger.trace("EXPORT" + dbexport + "EXPORT");

                    //we have somewhere to send information
                    String[] tmpStr = this.plugin.getGlobalControllerPath().split("_");

                    me = new MsgEvent(MsgEvent.Type.CONFIG, this.plugin.getRegion(), this.plugin.getAgent(), this.plugin.getPluginID(), "regionalimport");
                    me.setParam("action", "regionalimport");
                    me.setParam("globalcmd", Boolean.TRUE.toString());
                    me.setParam("src_region", this.plugin.getRegion());
                    me.setParam("dst_region", tmpStr[0]);
                    me.setParam("dst_agent", tmpStr[1]);
                    me.setParam("dst_plugin", plugin.getPluginID());
                    me.setParam("exportdata",dbexport);
                    this.plugin.msgIn(me);
                }

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
            if(plugin.isGlobalController()) { //only run if node is a global controller
                logger.debug("GlobalNodeStatusWatchDog");
                //update own database


                Map<String, NodeStatusType> nodeListStatus = plugin.getGDB().getNodeStatus(null, null, null);
                for (Map.Entry<String, NodeStatusType> entry : nodeListStatus.entrySet()) {

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

                    }

                }
            }
            else {
                //send full export of region to global controller
                //Need to develop a better inconsistency method
                //logger.info("Regional Export");
                //todo enable global export
                //regionalDBexport();

            }
        }
    }
}