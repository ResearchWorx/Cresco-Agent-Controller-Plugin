package com.researchworx.cresco.controller.globalscheduler;


import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.messaging.MsgEvent;
import com.researchworx.cresco.library.utilities.CLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

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
        this.logger = new CLogger(PollAddPlugin.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Debug);
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

	        	while((edge_id == null) && (count < 60))
	        	{
	        	    edge_id = plugin.getGDB().dba.getResourceEdgeId(resource_id,inode_id);
					/*
                    if (count == 0) {
                            Map<String, String> params = new HashMap<>();
                            params.put("cool", "stuff");
                            boolean isFound = false;
                            String pluginId = null;
                            while(!isFound) {
                                int pluginNum = ThreadLocalRandom.current().nextInt(0, 100);
                                pluginId = "plugin/" + String.valueOf(pluginNum);
                                if(plugin.getGDB().dba.getResourceEdgeId(resource_id,inode_id,region,agent,pluginId) == null) {
                                    isFound = true;
                                }
                            }
                            plugin.getGDB().dba.updateKPI(region,agent,pluginId, resource_id, inode_id, params);
                    }
					*/
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
	            logger.error("PollAddPlugin: " + ex.getMessage());
	       }
	    }  
}
