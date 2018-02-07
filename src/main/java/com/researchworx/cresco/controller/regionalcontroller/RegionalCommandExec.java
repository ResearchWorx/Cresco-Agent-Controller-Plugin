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
	//private AgentDiscovery regionalDiscovery;
	public GlobalCommandExec gce;

	public RegionalCommandExec(Launcher plugin)
	{
		this.logger = new CLogger(RegionalCommandExec.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
		this.plugin = plugin;
		//regionalDiscovery = new AgentDiscovery(plugin);
		gce = new GlobalCommandExec(plugin);
	}

	public MsgEvent execute(MsgEvent le) {

        if(le.getParam("is_global") != null) {
                //this is a global command
                if(plugin.cstate.isGlobalController()) {
                    return gce.execute(le);
                }
                else {
                    globalSend(le);
                    return null;
                }
            }

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
                        case "agent_disable":
                            logger.debug("CONFIG : AGENTDISCOVER REMOVE: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                            logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
                            plugin.getGDB().removeNode(le);
                            break;
                        case "agent_enable":
                            logger.debug("CONFIG : AGENT ADD: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                            logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
                            return enableAgent(le);
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
				//regionalDiscovery.discover(le);
				watchDogAgent(le);
        }
            else if (le.getMsgType() == MsgEvent.Type.INFO) {
                //logger.debug("INFO: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                //logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");
            }
			else if (le.getMsgType() == MsgEvent.Type.KPI) {
				logger.debug("KPI: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
				//logger.info("Send GLOBAL KPI: MsgType=" + le.getMsgType() + " Params=" + le.getParams());
                if(plugin.cstate.isGlobalController()) {
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

    private void watchDogAgent(MsgEvent le) {
        try {

                logger.debug("WATCHDOG : Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
                logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");

            plugin.getGDB().watchDogUpdate(le);
                //regionalDiscovery.discover(le);
                //TODO Agent Watchdog messages should not make their way to external GC.
                /*
                if((le.getParam("pluginconfigs") != null) && (!plugin.cstate.isGlobalController())) {
                    globalSend(le);
                }
                */

        } catch (Exception ex) {
            ex.printStackTrace();
            logger.debug("watchDogAgent : " + ex.toString());
        }
    }

	private MsgEvent enableAgent(MsgEvent le) {

        logger.debug("CONFIG : AGENTDISCOVER ADD: Region:" + le.getParam("src_region") + " Agent:" + le.getParam("src_agent"));
        logger.trace("Message Body [" + le.getMsgBody() + "] [" + le.getParams().toString() + "]");


        plugin.getGDB().addNode(le);

            if(!plugin.cstate.isGlobalController()) {
            //TODO Fix Global Send
                //globalSend(le);
        }

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
            if(!this.plugin.cstate.isGlobalController()) {
                    ge.setParam("dst_region",plugin.cstate.getGlobalRegion());
                    ge.setParam("dst_agent",plugin.cstate.getGlobalAgent());
                    ge.setParam("dst_plugin",plugin.cstate.getControllerId());
                    ge.setParam("globalcmd", Boolean.TRUE.toString());
                    plugin.sendAPMessage(ge);
            }
        }
        catch (Exception ex) {
            logger.error("globalSend : " + ex.getMessage());
        }
    }

}
