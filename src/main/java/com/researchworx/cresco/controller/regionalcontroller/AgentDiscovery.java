package com.researchworx.cresco.controller.regionalcontroller;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.messaging.RPC;
import com.researchworx.cresco.library.utilities.CLogger;

public class AgentDiscovery {
    private Launcher plugin;
    private CLogger logger;
    private static RPC rpc;

    public AgentDiscovery(Launcher plugin) throws Exception {
        this.plugin = plugin;
        logger = new CLogger(AgentDiscovery.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
        //rpc = new RPC(plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), null);
    }

    public void discover(MsgEvent le) {
        try {

            String discoverString = le.getParam("src_region") + "-" + le.getParam("src_agent") + "-" + le.getParam("src_plugin");
            logger.trace("MsgType: [" + le.getMsgType() + "] Params: [" + le.getParams() + "]");
            if (plugin.getDiscoveryMap().containsKey(discoverString)) {
                logger.info("Discovery underway for : discoverString=" + discoverString);
            } else {

                plugin.getDiscoveryMap().put(discoverString, System.currentTimeMillis());

                if (le.getMsgType() == MsgEvent.Type.CONFIG) {
                    if(le.getMsgBody().equals("disabled")) {
                        logger.debug("CONFIG : AGENTDISCOVER REMOVE: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                        logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
                        plugin.getGDB().removeNode(le);
                        le.setMsgBody("ack");
                        le.setReturn();
                        plugin.sendMsgEvent(le);
                    } else if (le.getMsgBody().equals("enabled")) {
                        logger.debug("CONFIG : AGENTDISCOVER ADD: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                        logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
                        plugin.getGDB().addNode(le);
                    }

                } else if (le.getMsgType() == MsgEvent.Type.WATCHDOG) {
                    logger.debug("WATCHDOG : AGENTDISCOVER: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
                    plugin.getGDB().watchDogUpdate(le);

                } else if (le.getMsgType() == MsgEvent.Type.KPI) {
                    logger.debug("KPI: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    //logger.info("MsgType=" + le.getMsgType() + " Params=" + le.getParams());
                    if (plugin.getGlobalControllerChannel() != null)
                        plugin.getGlobalControllerChannel().updatePerf(le);
                }
                else if (le.getMsgType() == MsgEvent.Type.INFO) {
                    logger.debug("INFO: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
                }
                else {
                    logger.error("UNKNOWN DISCOVERY PATH! : MsgType=" + le.getMsgType() + " " +  le.getParams());
                }
                if (plugin.getDiscoveryMap().containsKey(discoverString)) {
                    plugin.getDiscoveryMap().remove(discoverString); //remove discovery block
                }

            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.debug("Controller : AgentDiscovery run() : " + ex.toString());

        }
    }

}
