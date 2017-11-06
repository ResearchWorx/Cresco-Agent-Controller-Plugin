package com.researchworx.cresco.controller.globalscheduler;


import com.google.gson.reflect.TypeToken;
import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.core.WatchDog;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

public class PollAddPlugin implements Runnable {

	private String resource_id  = null;
	private String inode_id = null;
	private String region = null;
	private String agent = null;
	private Launcher plugin;
	private CLogger logger;
	private MsgEvent me;

	public PollAddPlugin(Launcher plugin, String resource_id, String inode_id, String region, String agent, MsgEvent me)
	{
        this.logger = new CLogger(PollAddPlugin.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Trace);
		this.plugin = plugin;
		this.resource_id = resource_id;
		this.inode_id = inode_id;
		this.region = region;
		this.agent = agent;
		this.me = me;

    }
	 public void run() {
         logger.debug("new thread for polladdplugin" );
         try
	        {
                int count = 0;
	        	String edge_id = null;
				//send message here
				logger.info("PollAddPlugin: Sending message: " + me.getParams());
                //String configParams = me.getParam("configparams");
				//me.setCompressedParam("configparams",configParams);
				MsgEvent re = plugin.sendRPC(me);
				logger.info("PollAddPlugin: Return message: " + re.getParams());

				//configparams
				String status_code_plugin = re.getParam("status_code");
				//logger.error("status_code: " + status_code_plugin);

				logger.info("PollAddPlugin: 1");

				String pluginId = re.getParam("plugin");

				Map<String,String> ihm = plugin.getGDB().gdb.getNodeParams(inode_id);
				logger.error("inode params: " + ihm.toString());


				String status_code = plugin.getGDB().gdb.getNodeParam(region,agent,pluginId,"status_code");
				//logger.info();
				logger.info("PollAddPlugin: Agent-Code: " + status_code_plugin + " DB-code:" + status_code);

				/*
				if(status_code.equals("-1")) {
					logger.error("status_code_db = " +status_code);
				}
				*/

				//addIsAttachedEdge(String resource_id, String inode_id, String region, String agent, String plugin)
				if(re != null) {


					logger.info("PollAddPlugin: Pre-inode: " + inode_id + " update");
//public boolean updateKPI(String region, String agent, String pluginId, String resource_id, String inode_id, Map<String,String> params) {
                    Map<String,String> params = new HashMap<>();
                    //params.put("region",region);
                    //params.put("agent", agent);
                    //params.put("plugin", pluginId);
                    params.put("init", String.valueOf(System.currentTimeMillis()));
                    plugin.getGDB().dba.updateKPI(region,agent,pluginId,resource_id,inode_id,params);
					logger.info("PollAddPlugin: Post-inode: " + inode_id + " update");

					/*

                    String resource_node_id = plugin.getGDB().dba.getResourceNodeId(resource_id);
                    String inode_node_id = plugin.getGDB().dba.getINodeNodeId(inode_id);
                    String plugin_node_id = plugin.getGDB().gdb.getNodeId(region,agent,pluginId);

                    logger.info("RESOURCE: resource_node_id:" + resource_node_id + " resource_id:" + resource_id);
                    logger.info("INODE: inode_node_id:" + inode_node_id + " inode_id:" + inode_id);
                    logger.info("AGENT: region:" + region + " agent:" + agent + " pluginId:" + pluginId + " pnodeid:" + plugin_node_id);

                    if((resource_node_id != null) && (inode_node_id != null) && (plugin_node_id != null)) {
                        edge_id = plugin.getGDB().dba.getResourceEdgeId(resource_id, inode_id, region, agent, pluginId);
                        if(edge_id == null)
                        {
                            logger.info("Adding Edge ID");
                            edge_id = plugin.getGDB().dba.addIsAttachedEdge(resource_id, inode_id, region, agent, pluginId);
                            Map<String,String> params = new HashMap<>();
                            plugin.getGDB().dba.updateEdgeNoTx(edge_id, params);


                            logger.info("Edge ID ADD: " + edge_id);
                            logger.debug("PollAddPlugin edge addIsAttachedEdge resource_node_id " + resource_id + " inode_id " + inode_id + "  Node" + region + " " + agent + " " + plugin + " = " + plugin_node_id);
                        } else {
                            logger.info("Edge ID ELSE: " + edge_id);
                        }

                    }
                    */
                    /*
                    String resource_node_id = plugin.getGDB().dba.getResourceNodeId(resource_id);
                    String inode_node_id = plugin.getGDB().dba.getINodeNodeId(inode_id);
                    String plugin_node_id = plugin.getGDB().gdb.getNodeId(region,agent,pluginId);

                    //create node if not seen.. this needs to be changed.
                    if(plugin_node_id == null) {
                        plugin_node_id = plugin.getGDB().gdb.addNode(region,agent,pluginId);
                        logger.debug("PollAddPlugin : Added Node" + region + " " + agent + " " + pluginId + " = " + plugin_node_id);
                    }

                    if((resource_node_id != null) && (inode_node_id != null) && (plugin_node_id != null)) {
                        edge_id = plugin.getGDB().dba.getResourceEdgeId(resource_id, inode_id, region, agent, pluginId);
                        if(edge_id == null)
                        {
                            edge_id = plugin.getGDB().dba.addIsAttachedEdge(resource_id, inode_id, region, agent, pluginId);
                            logger.debug("PollAddPlugin edge addIsAttachedEdge resource_node_id " + resource_id + " inode_id " + inode_id + "  Node" + region + " " + agent + " " + plugin + " = " + plugin_node_id);
                        }

                    }
                    */
                }

				while((edge_id == null) && (count < 300))
	        	{
	        		logger.info("inode_id: " + inode_id + " edge_id:" + edge_id);
	        	    edge_id = plugin.getGDB().dba.getResourceEdgeId(resource_id,inode_id);
                    Thread.sleep(1000);
                    count++;
	        	}

	        	if(edge_id != null)
	        	{
                    plugin.getGDB().dba.setINodeParam(inode_id,"status_code","10");
                    plugin.getGDB().dba.setINodeParam(inode_id,"status_desc","iNode Active.");
                    logger.debug("ResourceSchedulerEngine : pollAddPlugin : Activated inode_id=" + inode_id);

	        	}
	        	else
	        	{
	        		logger.debug("ResourceSchedulerEngine : pollAddPlugin : unable to verify iNode activation!  inode_id=" + inode_id);
                    plugin.getGDB().dba.setINodeParam(inode_id,"status_code","40");
                    plugin.getGDB().dba.setINodeParam(inode_id,"status_desc","iNode Failed Scheduling.");

                }

	        }
		   catch(Exception ex)
		   {
               logger.debug("ResourceSchedulerEngine : pollAddPlugin : unable to verify iNode activation!  inode_id=" + inode_id);
               plugin.getGDB().dba.setINodeParam(inode_id,"status_code","41");
               plugin.getGDB().dba.setINodeParam(inode_id,"status_desc","iNode Failed Scheduling Exception.");

               logger.error("PollAddPlugin: Error " + ex.getMessage());
               StringWriter errors = new StringWriter();
               ex.printStackTrace(new PrintWriter(errors));
               logger.error("PollAddPlugin: Trace " + errors.toString());

           }
	    }

}
