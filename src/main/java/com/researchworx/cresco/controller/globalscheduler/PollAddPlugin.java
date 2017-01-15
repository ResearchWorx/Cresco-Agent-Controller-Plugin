package com.researchworx.cresco.controller.globalscheduler;


import com.researchworx.cresco.controller.core.Launcher;
import com.researchworx.cresco.library.utilities.CLogger;

public class PollAddPlugin implements Runnable {

	private String resource_id  = null;
	private String inode_id = null;
	private String region = null;
	private String agent = null;
	private Launcher plugin;
	private CLogger logger;

	public PollAddPlugin(Launcher plugin, String resource_id, String inode_id, String region, String agent)
	{
		this.logger = new CLogger(PollAddPlugin.class, plugin.getMsgOutQueue(), plugin.getRegion(), plugin.getAgent(), plugin.getPluginID(), CLogger.Level.Debug);
		this.plugin = plugin;
		this.resource_id = resource_id;
		this.inode_id = inode_id;
		this.region = region;
		this.agent = agent;
		
	}
	 public void run() {
	        try 
	        {
	        	int count = 0;
	        	String edge_id = null;
	        	while((edge_id == null) && (count < 30))
	        	{
	        		edge_id = plugin.getGDB().gdb.getResourceEdgeId(resource_id, inode_id, region, agent);
	        		Thread.sleep(1000);
	        		count++;
	        	}
	        	if(edge_id != null)
	        	{
	        		if((plugin.getGDB().gdb.setINodeParam(resource_id,inode_id,"status_code","10")) &&
							(plugin.getGDB().gdb.setINodeParam(resource_id,inode_id,"status_desc","iNode Active.")))
					{
							//recorded plugin activations
	        				logger.debug("ResourceSchedulerEngine : pollAddPlugin : Activated inode_id=" + inode_id);
					}
	        	}
	        	else
	        	{
	        		logger.debug("ResourceSchedulerEngine : pollAddPlugin : unable to verify iNode activation!");
	        	}
	        }
		   catch(Exception v) 
		   {
	            logger.error(v.getMessage());
	       }
	    }  
}
