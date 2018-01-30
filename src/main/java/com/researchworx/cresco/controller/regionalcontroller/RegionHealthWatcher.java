package com.researchworx.cresco.controller.regionalcontroller;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.db.NodeStatusType;
import com.researchworx.cresco.library.core.WatchDog;
import com.researchworx.cresco.library.utilities.CLogger;
import jdk.nashorn.internal.runtime.regexp.joni.constants.NodeStatus;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class RegionHealthWatcher {
    public Timer timer;
    private Launcher plugin;
    private CLogger logger;
    private long startTS;
    private int wdTimer;
    private Timer regionalUpdateTimer;
    public RegionalCommandExec rce;

    //private static final Logger logger = LoggerFactory.getLogger(HealthWatcher.class);

    public RegionHealthWatcher(Launcher plugin) {
        this.logger = new CLogger(RegionHealthWatcher.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
        rce = new RegionalCommandExec(plugin);
        
        logger.debug("Initializing");
        this.plugin = plugin;
        wdTimer = 1000;
        startTS = System.currentTimeMillis();
        timer = new Timer();
        timer.scheduleAtFixedRate(new CommunicationHealthWatcherTask(), 1000, wdTimer);
        regionalUpdateTimer = new Timer();
        regionalUpdateTimer.scheduleAtFixedRate(new RegionHealthWatcher.RegionalNodeStatusWatchDog(plugin, logger), 15000, 15000);//remote
        logger.info("Initialized");

    }

    public void shutdown() {
        timer.cancel();
        logger.debug("Shutdown");
    }

    class CommunicationHealthWatcherTask extends TimerTask {
        public void run() {
            boolean isHealthy = true;
            try {
                if (!plugin.isConsumerThreadActive() || !plugin.getConsumerAgentThread().isAlive()) {
                    isHealthy = false;
                    logger.info("Agent Consumer shutdown detected");
                }

                if (plugin.isRegionalController()) {
                    if (!plugin.isDiscoveryActive()) {
                        isHealthy = false;
                        logger.info("Discovery shutdown detected");

                    }
                    if (!(plugin.isConsumerThreadRegionActive()) || !(plugin.getConsumerRegionThread().isAlive())) {
                        isHealthy = false;
                        logger.info("Region Consumer shutdown detected");
                    }
                    if (!(plugin.isActiveBrokerManagerActive()) || !(plugin.getActiveBrokerManagerThread().isAlive())) {
                        isHealthy = false;
                        logger.info("Active Broker Manager shutdown detected");
                    }
                    if (!plugin.getBroker().isHealthy()) {
                        isHealthy = false;
                        logger.info("Broker shutdown detected");
                    }

                }

                if (!isHealthy) {
                    plugin.removeGDBNode(plugin.getRegion(), plugin.getAgent(), null); //remove self from DB
                    logger.info("System has become unhealthy, rebooting services");
                    plugin.setRestartOnShutdown(true);
                    plugin.closeCommunications();
                }
            } catch (Exception ex) {
                logger.error("Run {}", ex.getMessage());
                ex.printStackTrace();
            }
        }
    }

    class RegionalNodeStatusWatchDog extends TimerTask {
        private CLogger logger;
        private Launcher plugin;
        public RegionalNodeStatusWatchDog(Launcher plugin, CLogger logger) {
            this.plugin = plugin;
            this.logger = logger;
        }
        public void run() {
            if(plugin.isRegionalController()) { //only run if node is regional controller
                logger.debug("RegionalNodeStatusWatchDog");
                Map<String, NodeStatusType> nodeStatus = plugin.getGDB().getNodeStatus(plugin.getRegion(), null, null);
                for (Map.Entry<String, NodeStatusType> entry : nodeStatus.entrySet()) {
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
                        Map<String,String> nodeParams = plugin.getGDB().gdb.getNodeParams(entry.getKey());
                        for (Map.Entry<String, String> entry2 : nodeParams.entrySet()) {
                            logger.error("Key = " + entry2.getKey() + ", Value = " + entry2.getValue());
                        }
                        String region = nodeParams.get("region");
                        String agent = nodeParams.get("agent");
                        String pluginId = nodeParams.get("plugin");
                        logger.error("Problem with " + region + " " + agent + " " + pluginId);
                        logger.error("NodeID : " + entry.getKey() + " Status : " + entry.getValue().toString());

                        //String region = nodeParams.get("region");
                        //String agent = nodeParams.get("agent");
                        //String pluginId = nodeParams.get("plugin");
                        //logger.error("Error " + region + " " + agent + " " + pluginId);

                    }

                }
            }
        }
    }

}
