package com.researchworx.cresco.controller.globalscheduler;


import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.core.WatchDog;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.io.PrintWriter;
import java.io.StringWriter;

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
        this.logger = new CLogger(PollAddPlugin.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Info);
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
                String configParams = me.getParam("configparams");
				me.setCompressedParam("configparams",configParams);
				MsgEvent re = plugin.sendRPC(me);
                //addIsAttachedEdge(String resource_id, String inode_id, String region, String agent, String plugin)
				if(re != null) {
                    String pluginId = re.getParam("plugin");
                    //configparams

                    String resource_node_id = plugin.getGDB().dba.getResourceNodeId(resource_id);
                    String inode_node_id = plugin.getGDB().dba.getINodeNodeId(inode_id);
                    String plugin_node_id = plugin.getGDB().gdb.getNodeId(region,agent,pluginId);

                    //create node if not seen.. this needs to be changed.
                    if(plugin_node_id == null) {
                        plugin_node_id = plugin.getGDB().gdb.addNode(region,agent,pluginId);
                        logger.debug("PollAddPlugin : Added Node" + region + " " + agent + " " + pluginId + " = " + plugin_node_id);
                    }
                    //provide record of submitted plugin
                    plugin.getGDB().gdb.setNodeParam(plugin_node_id,"configparams",configParams);

                    if((resource_node_id != null) && (inode_node_id != null) && (plugin_node_id != null)) {
                        edge_id = plugin.getGDB().dba.getResourceEdgeId(resource_id, inode_id, region, agent, pluginId);
                        if(edge_id == null)
                        {
                            edge_id = plugin.getGDB().dba.addIsAttachedEdge(resource_id, inode_id, region, agent, pluginId);
                            logger.debug("PollAddPlugin edge addIsAttachedEdge resource_node_id " + resource_id + " inode_id " + inode_id + "  Node" + region + " " + agent + " " + plugin + " = " + plugin_node_id);
                        }

                    }


                }
				logger.info("PollAddPlugin: Return message: " + re.getParams());

				while((edge_id == null) && (count < 300))
	        	{
	        		logger.trace("inode_id: " + inode_id + " edge_id:" + edge_id);
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
