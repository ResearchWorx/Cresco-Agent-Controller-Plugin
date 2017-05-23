package com.researchworx.cresco.controller.regionalcontroller;


import com.researchworx.cresco.controller.app.gPayload;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.controller.globalcontroller.GlobalCommandExec;
import com.researchworx.cresco.controller.globalscheduler.PollRemovePipeline;
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

	        //logger.error("INCOMING: " + le.getParams().toString());


            if(le.getParam("globalcmd") != null) {
                //this is a global command
                if(plugin.isGlobalController()) {
                    return gce.execute(le);
                }
                else {
                    globalSend(le);
                    return null;
                }
            }
			else if(le.getMsgType() == MsgEvent.Type.CONFIG) {
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
                            logger.debug("CONFIG : AGENTDISCOVER ADD: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                            logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
                            plugin.getGDB().addNode(le);
                            le.setParam("globalcmd", Boolean.TRUE.toString());
                            globalSend(le);
                            break;
                        default:
                            logger.debug("Unknown configtype found: {}", le.getParam("action"));
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
				//logger.info("MsgType=" + le.getMsgType() + " Params=" + le.getParams());
				if(plugin.isGlobalController()) {
					logger.debug("MsgType=" + le.getMsgType() + " Params=" + le.getParams());
					plugin.getGDB().updateKPI(le);
				}
				else {
					globalSend(le);
				}

			}

			else {
				logger.error("UNKNOWN MESSAGE! : MsgType=" + le.getMsgType() + " " +  le.getParams());
			}

		return null;
	}

    private void globalSend(MsgEvent ge) {
        try {
            if(!this.plugin.isGlobalController()) {
                if(this.plugin.getGlobalControllerPath() != null) {
                    String[] tmpStr = this.plugin.getGlobalControllerPath().split("_");
                    ge.setParam("dst_region", tmpStr[0]);
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
