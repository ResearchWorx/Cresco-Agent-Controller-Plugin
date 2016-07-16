package com.researchworx.cresco.controller.regionalcontroller;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.Timer;
import java.util.TimerTask;

public class HealthWatcher {
    public Timer timer;
    private Launcher plugin;
    private CLogger logger;
    private long startTS;
    private int wdTimer;
    //private static final Logger logger = LoggerFactory.getLogger(HealthWatcher.class);

    public HealthWatcher(Launcher plugin) {
        this.logger = new CLogger(HealthWatcher.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
        logger.debug("Initializing");
        this.plugin = plugin;
        wdTimer = 1000;
        startTS = System.currentTimeMillis();
        timer = new Timer();
        timer.scheduleAtFixedRate(new HealthWatcherTask(), 500, wdTimer);
    }

    public void shutdown() {
        timer.cancel();
        logger.debug("Shutdown");
    }

    class HealthWatcherTask extends TimerTask {
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
}
