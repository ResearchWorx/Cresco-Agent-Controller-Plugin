package com.researchworx.cresco.controller.regionalcontroller;

import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalcontroller.GlobalCommandExec;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.messaging.RPC;
import com.researchworx.cresco.library.utilities.CLogger;

public class AgentDiscovery {
    private Launcher plugin;
    private CLogger logger;
    private RPC rpc;
    private GlobalCommandExec gce;

    public AgentDiscovery(Launcher plugin) {
        this.plugin = plugin;
        logger = new CLogger(AgentDiscovery.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
        gce = new GlobalCommandExec(plugin);
        //rpc = new RPC(plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), null);
    }

    //function to send to global controller
    private void globalSend(MsgEvent ge) {
        try {
            if(!this.plugin.isGlobalController()) {
                if(this.plugin.getGlobalControllerPath() != null) {
                    String[] tmpStr = this.plugin.getGlobalControllerPath().split("_");
                    ge.setParam("dst_region", tmpStr[0]);
                    ge.setParam("dst_plugin", plugin.getPluginID());
                    plugin.msgIn(ge);
                }
            }
        }
        catch (Exception ex) {
            logger.error("globalSend : " + ex.getMessage());
        }
    }

    public void discover(MsgEvent le) {
        try {

            String discoverString = le.getParam("src_region") + "-" + le.getParam("src_agent") + "-" + le.getParam("src_plugin");
            logger.trace("MsgType: [" + le.getMsgType() + "] Params: [" + le.getParams() + "]");
            if (plugin.getDiscoveryMap().containsKey(discoverString)) {
                logger.debug("Discovery underway for : discoverString=" + discoverString);
            } else {

                plugin.getDiscoveryMap().put(discoverString, System.currentTimeMillis());

                logger.debug("WATCHDOG : AGENTDISCOVER: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
                plugin.getGDB().watchDogUpdate(le);


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
