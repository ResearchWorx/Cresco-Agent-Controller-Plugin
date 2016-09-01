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
            logger.debug("MsgType : " + le.getMsgType() + " Params: " + le.getParams());
            if (plugin.getDiscoveryMap().containsKey(discoverString)) {
                logger.info("Discovery underway for : discoverString=" + discoverString);
            } else {

                plugin.getDiscoveryMap().put(discoverString, System.currentTimeMillis());

                if ((le.getMsgType() == MsgEvent.Type.CONFIG) && (le.getMsgBody().equals("disabled"))) {
                    //if we see a agent enable command respond to it

                    logger.debug("Remove Node: " + le.getParams());
                    if (le.getParam("src_plugin") == null) //if plugin discover plugin info as well
                    {
                        plugin.getGDB().removeNode(le.getParam("src_region"), le.getParam("src_agent"), null);
                        logger.debug("AGENT REMOVED: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    } else {
                        plugin.getGDB().removeNode(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"));
                        logger.debug("PLUGIN REMOVED: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent") + " Plugin:" + le.getParam("src_plugin"));

                    }

                } else if ((le.getMsgType() == MsgEvent.Type.CONFIG) && (le.getMsgBody().equals("enabled"))) {
                    //if we see a agent enable command respond to it
                    plugin.getGDB().addNode(le);
                    logger.debug("CONFIG : AGENTDISCOVER: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    le.setMsgPlugin(null);
                    le.setMsgRegion(le.getParam("src_region"));
                    le.setMsgAgent(le.getParam("src_agent"));
                    le.removeParam("src_plugin");
                    le.setMsgBody("controllerenabled");
                    le.setParam("dst_region", le.getParam("src_region"));
                    le.setParam("dst_agent", le.getParam("src_agent"));
                    le.setSrc(plugin.getRegion(), plugin.getAgent(), plugin.getPluginID());
                    //le.setDst(me.getParam("src_region"),me.getParam("src_agent"),me.getParam("src_plugin"));
                    plugin.sendMsgEvent(le);
                } else if (le.getMsgType() == MsgEvent.Type.WATCHDOG) {
                    logger.debug("WATCHDOG : AGENTDISCOVER: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    try {
                        plugin.getGDB().addNode(le);
                        /*
                        if ((le.getParam("src_region") != null) && (le.getParam("src_agent") != null) && (le.getParam("src_plugin")) == null) { //agent
                            if (!plugin.getGDB().isNode(le.getParam("src_region"), le.getParam("src_agent"), null)) { //add if it does not exist
                                plugin.getGDB().addNode(le.getParam("src_region"), le.getParam("src_agent"), null, le.getParams());
                            }
                        } else if ((le.getParam("src_region") != null) && (le.getParam("src_agent") != null) && (le.getParam("src_plugin")) != null) { //plugin
                            if (!plugin.getGDB().isNode(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"))) { //add if it does not exist
                                plugin.getGDB().addNode(le.getParam("src_region"), le.getParam("src_agent"), le.getParam("src_plugin"), le.getParams());
                            }
                        }
                        */
                    } catch (Exception ex) {
                        logger.debug("WATCHDOG : " + ex.getMessage());
                    }

                } else if (le.getMsgType() == MsgEvent.Type.KPI) {
                    logger.debug("KPI: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                    //logger.info("MsgType=" + le.getMsgType() + " Params=" + le.getParams());
                    if (plugin.getGlobalControllerChannel() != null)
                        plugin.getGlobalControllerChannel().updatePerf(le);
                }
                else if (le.getMsgType() == MsgEvent.Type.INFO) {
                    logger.debug("INFO: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
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
