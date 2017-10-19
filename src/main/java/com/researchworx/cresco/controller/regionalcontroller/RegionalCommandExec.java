package com.researchworx.cresco.controller.regionalcontroller;


import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.researchworx.cresco.controller.app.gPayload;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalcontroller.GlobalCommandExec;
import com.researchworx.cresco.controller.globalscheduler.PollRemovePipeline;
import com.researchworx.cresco.library.core.WatchDog;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.jar.*;

public class RegionalCommandExec {

	private Launcher plugin;
	private CLogger logger;
	private AgentDiscovery regionalDiscovery;
	private GlobalCommandExec gce;

	public RegionalCommandExec(Launcher plugin)
	{
		this.logger = new CLogger(RegionalCommandExec.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
		regionalDiscovery = new AgentDiscovery(plugin);
		gce = new GlobalCommandExec(plugin);
	}

	public MsgEvent execute(MsgEvent le) {

        if(le.getParam("globalcmd") == null) {
            //not for global controller, send to region
            if (le.getParam("dst_agent") != null) {
                regionSend(le);
                return null;
            }
        }

	    //Add Region specific information for return information
        le.setParam("dst_agent",plugin.getAgent());
        le.setParam("dst_plugin",plugin.getPluginID());


        if(le.getParam("globalcmd") != null) {
                //this is a global command
                if(plugin.isGlobalController()) {
                    logger.error("SEND GLOBAL: " + le.getMsgType() + " params: " + le.getParams());
                    return gce.execute(le);
                }
                else {
                    globalSend(le);
                    return null;
                }
            }

            //le.setParam("dst_agent",plugin.getAgent());
            //le.setParam("dst_plugin",plugin.getPluginID());



        if(le.getMsgType() == MsgEvent.Type.EXEC) {
            if(le.getParam("action") != null) {
                switch (le.getParam("action")) {

                        case "ping":
                            return pingReply(le);

                        default:
                            logger.error("RegionalCommandExec Unknown configtype found {} for {}:", le.getParam("action"), le.getMsgType().toString());
                            return null;
                }
            }
            } else if(le.getMsgType() == MsgEvent.Type.CONFIG) {
                if(le.getParam("action") != null) {
                    switch (le.getParam("action")) {
                        case "disable":
                            logger.debug("CONFIG : AGENTDISCOVER REMOVE: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                            logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
                            plugin.getGDB().removeNode(le);
                            le.setParam("globalcmd", Boolean.TRUE.toString());
                            globalSend(le);
                            break;
                        case "enable":
                            enableAgent(le);
                            break;
                        default:
                            logger.debug("RegionalCommandExec Unknown configtype found: {}", le.getParam("action"));
                            return null;
                    }

                }
                else {
                    logger.error("CONFIG : UNKNOWN ACTION: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent") + " " +  le.getParams());
                    //return gce.cmdExec(le);
                }
			}
			else if(le.getMsgType() == MsgEvent.Type.WATCHDOG) {
				regionalDiscovery.discover(le);
			}
            else if (le.getMsgType() == MsgEvent.Type.INFO) {
                //logger.debug("INFO: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                //logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
            }
			else if (le.getMsgType() == MsgEvent.Type.KPI) {
				logger.debug("KPI: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
				//logger.info("Send GLOBAL KPI: MsgType=" + le.getMsgType() + " Params=" + le.getParams());
                if(plugin.isGlobalController()) {
                    return gce.execute(le);
                }
                else {
                    if(plugin.getConfig().getBooleanParam("forward_global_kpi",true)){
                        globalSend(le);
                    }
                    return null;
                }
			}

			else {
				logger.error("RegionalCommandExec UNKNOWN MESSAGE! : MsgType=" + le.getMsgType() + " " +  le.getParams());
			}

		return null;
	}

	private MsgEvent enableAgent(MsgEvent le) {

	    /*
        if(le.getMsgType().equals(MsgEvent.Type.CONFIG)) {
            logger.error("WTF: " + le.getParams());
        }
        */

        logger.debug("CONFIG : AGENTDISCOVER ADD: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
        logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
        plugin.getGDB().addNode(le);

        //gdb.addNode(region, agent,plugin);
        //gdb.setNodeParams(region,agent,plugin, de.getParams());

        //process agent configs

        //whut
        //le.setParam("globalcmd", Boolean.TRUE.toString());
        //globalSend(le);

        return le;
    }

    private MsgEvent pingReply(MsgEvent msg) {
        logger.debug("ping message type found");
        msg.setParam("action","pong");
        msg.setParam("remote_ts", String.valueOf(System.currentTimeMillis()));
        msg.setParam("type", "agent_controller");
        logger.debug("Returning communication details to Cresco agent");
        return msg;
    }

    private void regionSend(MsgEvent rs) {
        plugin.sendAPMessage(rs);
    }

    private void globalSend(MsgEvent ge) {
        try {
            if(!this.plugin.isGlobalController()) {
                if(this.plugin.getGlobalControllerPath() != null) {
                    String[] tmpStr = this.plugin.getGlobalControllerPath().split("_");
                    ge.setParam("dst_region", tmpStr[0]);
                    ge.removeParam("dst_agent");
                    ge.removeParam("dst_plugin");
                    //ge.setParam("dst_agent", tmpStr[1]);
                    //ge.setParam("dst_plugin", plugin.getPluginID());
                    //plugin.msgIn(ge);
                    plugin.sendAPMessage(ge);
                }
            }
        }
        catch (Exception ex) {
            logger.error("globalSend : " + ex.getMessage());
        }
    }

	/*
    //function to send to global controller
    private void globalSend(MsgEvent ge) {
        try {
            if(!this.plugin.isGlobalController()) {
                if(this.plugin.getGlobalControllerPath() != null) {
                    logger.error("PLUGIN PATH" + this.plugin.getGlobalControllerPath());
                    String[] tmpStr = this.plugin.getGlobalControllerPath().split("_");
                    ge.setParam("dst_region", tmpStr[0]);
                    //ge.setParam("dst_plugin", plugin.getPluginID());
                    logger.error("FORWARDING TO GC : " + ge.getParams().toString());
                    plugin.sendMsgEvent(ge);
                }
            }
        }
        catch (Exception ex) {
            logger.error("globalSend : " + ex.getMessage());
        }
    }
    */

}
