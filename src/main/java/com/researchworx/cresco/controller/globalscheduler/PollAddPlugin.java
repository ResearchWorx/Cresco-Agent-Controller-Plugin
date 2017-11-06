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
         try
	        {
                int count = 0;
	        	String edge_id = null;
				MsgEvent re = plugin.sendRPC(me);

				if(re != null) {
					//info returned from agent
					String pluginId = re.getParam("plugin");
					plugin.getGDB().dba.addIsAttachedEdge(resource_id, inode_id, region, agent, pluginId);

					String status_code_plugin = re.getParam("status_code");
					String status_desc_plugin = re.getParam("status_desc");

					if(Integer.parseInt(status_code_plugin) == 10) {
						plugin.getGDB().dba.setINodeParam(inode_id,"status_code","10");
						plugin.getGDB().dba.setINodeParam(inode_id,"status_desc","iNode Active.");
					} else {
						plugin.getGDB().dba.setINodeParam(inode_id, "status_code", status_code_plugin);
						plugin.getGDB().dba.setINodeParam(inode_id, "status_desc", status_desc_plugin);
					}
				} else {
					logger.debug("pollAddPlugin : unable to verify iNode activation!  inode_id=" + inode_id);
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
